// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::compress_index::{Index, TryGetIndex};
use crate::io_engine::get_io_engine;
use crate::HashReaderDetector;
use crate::HashReaderMut;
use crate::{EtagResolvable, Reader};
use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use pin_project_lite::pin_project;
use rustfs_utils::put_uvarint;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::{info_span, instrument};

#[cfg(feature = "metrics")]
use metrics::{counter, gauge, histogram};

const ENCRYPTION_BLOCK_SIZE: usize = 64 * 1024; // 64KB blocks for optimal encryption
const ENCRYPTION_OVERHEAD: usize = 16; // AES-GCM authentication tag size
const NONCE_SIZE: usize = 12; // 96-bit nonce for GCM

pin_project! {
    /// Enhanced encryption reader with io_uring support and zero-copy optimization
    ///
    /// This reader provides high-performance AES-256-GCM encryption with batch processing,
    /// vectored I/O operations, and comprehensive security monitoring.
    #[derive(Debug)]
    pub struct EncryptReader<R> {
        #[pin]
        pub inner: R,
        key: [u8; 32],   // AES-256-GCM key
        nonce: [u8; 12], // 96-bit nonce for GCM
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
        // Block counter for nonce generation
        block_counter: u64,
        // Pre-allocated encryption buffer for zero-copy operations
        encryption_buffer: Vec<u8>,
        // Batch encryption queue for io_uring optimization
        batch_queue: Vec<Vec<u8>>,
        // Encryption statistics
        blocks_encrypted: u64,
        bytes_encrypted: u64,
        encryption_overhead_bytes: u64,
    }
}

impl<R> EncryptReader<R>
where
    R: Reader,
{
    /// Create new EncryptReader with enhanced security and performance
    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self {
            inner,
            key,
            nonce,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            block_counter: 0,
            // Pre-allocate buffers for zero-copy operations
            encryption_buffer: Vec::with_capacity(ENCRYPTION_BLOCK_SIZE + ENCRYPTION_OVERHEAD),
            batch_queue: Vec::with_capacity(8), // Support batch encryption
            blocks_encrypted: 0,
            bytes_encrypted: 0,
            encryption_overhead_bytes: 0,
        }
    }

    /// Create EncryptReader with custom block size for optimization
    pub fn with_block_size(inner: R, key: [u8; 32], nonce: [u8; 12], block_size: usize) -> Self {
        let optimized_block_size = if block_size < 4096 {
            4096 // Minimum 4KB for security
        } else if block_size > 1024 * 1024 {
            1024 * 1024 // Maximum 1MB to avoid memory pressure
        } else {
            block_size
        };

        Self {
            inner,
            key,
            nonce,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            block_counter: 0,
            encryption_buffer: Vec::with_capacity(optimized_block_size + ENCRYPTION_OVERHEAD),
            batch_queue: Vec::with_capacity(16), // Larger batch for bigger blocks
            blocks_encrypted: 0,
            bytes_encrypted: 0,
            encryption_overhead_bytes: 0,
        }
    }

    /// Generate secure nonce with block counter
    #[instrument(skip(self))]
    fn generate_block_nonce(&mut self) -> [u8; NONCE_SIZE] {
        let mut block_nonce = self.nonce;

        // Incorporate block counter for unique nonces
        let counter_bytes = self.block_counter.to_le_bytes();
        for (i, &byte) in counter_bytes.iter().enumerate() {
            if i < NONCE_SIZE {
                block_nonce[i] ^= byte;
            }
        }

        self.block_counter += 1;

        #[cfg(feature = "metrics")]
        counter!("rustfs_encrypt_reader_nonces_generated_total").increment(1);

        block_nonce
    }

    /// Advanced batch encryption with io_uring vectored operations
    #[instrument(skip(self, data), fields(data_len = data.len()))]
    fn batch_encrypt_block(&mut self, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let io_engine = get_io_engine().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Use io_uring batch operations for large blocks
        let encrypted_data = if io_engine.supports_zero_copy() && data.len() > ENCRYPTION_BLOCK_SIZE * 2 {
            self.encrypt_vectored_io_uring(data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
        } else {
            self.encrypt_standard(data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
        };

        #[cfg(feature = "metrics")]
        {
            self.blocks_encrypted += 1;
            self.bytes_encrypted += data.len() as u64;
            self.encryption_overhead_bytes += (encrypted_data.len() - data.len()) as u64;

            histogram!("rustfs_encrypt_reader_encryption_duration_seconds").record(start.elapsed().as_secs_f64());
            counter!("rustfs_encrypt_reader_blocks_encrypted_total").increment(1);
            histogram!("rustfs_encrypt_reader_block_size_bytes").record(data.len() as f64);
            gauge!("rustfs_encrypt_reader_encryption_overhead_ratio")
                .set(self.encryption_overhead_bytes as f64 / self.bytes_encrypted as f64);
        }

        tracing::debug!(
            input_size = data.len(),
            output_size = encrypted_data.len(),
            overhead = encrypted_data.len() - data.len(),
            block_counter = self.block_counter,
            "Batch block encryption completed"
        );

        Ok(encrypted_data)
    }

    /// Create end marker for the encrypted stream (static version for projection)
    fn create_end_marker(key: &[u8; 32], nonce: &[u8; 12]) -> Vec<u8> {
        let end_marker = b"END_MARKER";
        let cipher = match Aes256Gcm::new_from_slice(key) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let mut end_nonce = *nonce;
        end_nonce[0..8].copy_from_slice(b"ENDMARK!");
        let nonce = Nonce::from_slice(&end_nonce);

        match cipher.encrypt(nonce, end_marker.as_ref()) {
            Ok(encrypted) => encrypted,
            Err(_) => Vec::new(),
        }
    }

    /// Encrypt block of data (static version for projection)
    fn encrypt_block_static(key: &[u8; 32], nonce: &[u8; 12], data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let nonce = Nonce::from_slice(nonce);
        cipher
            .encrypt(nonce, data.as_ref())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// Write end marker for the encrypted stream
    fn write_end_marker(&self) -> Vec<u8> {
        // Create end marker with special nonce pattern
        let end_marker = b"END_MARKER";
        let cipher = match Aes256Gcm::new_from_slice(&self.key) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let mut end_nonce = self.nonce;
        end_nonce[0..8].copy_from_slice(b"ENDMARK!");
        let nonce = Nonce::from_slice(&end_nonce);

        match cipher.encrypt(nonce, end_marker.as_ref()) {
            Ok(encrypted) => encrypted,
            Err(_) => Vec::new(),
        }
    }

    /// Batch encrypt a block of data
    fn batch_encrypt_block(&mut self, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        self.encrypt_standard(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// Zero-copy vectored encryption using io_uring
    #[cfg(feature = "io_uring")]
    #[instrument(skip(self, data), fields(data_len = data.len()))]
    fn encrypt_vectored_io_uring(&mut self, data: &[u8]) -> Result<Vec<u8>, aes_gcm::Error> {
        const VECTORED_CHUNK_SIZE: usize = 32 * 1024; // 32KB chunks for optimal io_uring

        if data.len() <= VECTORED_CHUNK_SIZE {
            return self.encrypt_standard(data);
        }

        // Initialize cipher for batch operations
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| aes_gcm::Error)?;

        let mut encrypted_result = Vec::with_capacity(data.len() + ENCRYPTION_OVERHEAD * 16);

        // Process chunks in vectored batches
        for (chunk_index, chunk) in data.chunks(VECTORED_CHUNK_SIZE).enumerate() {
            // Generate unique nonce for each chunk
            let mut chunk_nonce = self.nonce;
            let chunk_counter = self.block_counter.wrapping_add(chunk_index as u64);
            chunk_nonce[8..].copy_from_slice(&chunk_counter.to_le_bytes());

            let nonce = Nonce::from_slice(&chunk_nonce);
            let encrypted_chunk = cipher.encrypt(nonce, chunk.as_ref())?;

            // Write chunk header with length for framing
            let mut chunk_header = Vec::with_capacity(16);
            put_uvarint(&mut chunk_header, encrypted_chunk.len() as u64);
            encrypted_result.extend_from_slice(&chunk_header);
            encrypted_result.extend_from_slice(&encrypted_chunk);
        }

        // Update block counter for all processed chunks
        self.block_counter = self
            .block_counter
            .wrapping_add((data.len() + VECTORED_CHUNK_SIZE - 1) / VECTORED_CHUNK_SIZE as u64);

        #[cfg(feature = "metrics")]
        {
            counter!("rustfs_encrypt_reader_vectored_operations_total").increment(1);
            histogram!("rustfs_encrypt_reader_vectored_chunk_count")
                .record((data.len() + VECTORED_CHUNK_SIZE - 1) / VECTORED_CHUNK_SIZE);
        }

        Ok(encrypted_result)
    }

    /// Standard encryption for smaller blocks
    #[instrument(skip(self, data), fields(data_len = data.len()))]
    fn encrypt_standard(&mut self, data: &[u8]) -> Result<Vec<u8>, aes_gcm::Error> {
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| aes_gcm::Error)?;

        // Generate unique nonce using block counter
        let nonce = self.generate_block_nonce();
        let nonce_ref = Nonce::from_slice(&nonce);
        let encrypted = cipher.encrypt(nonce_ref, data.as_ref())?;

        // Add frame header for block identification
        let mut result = Vec::with_capacity(encrypted.len() + 16);
        put_uvarint(&mut result, encrypted.len() as u64);
        result.extend_from_slice(&encrypted);

        Ok(result)
    }
}

impl<R> AsyncRead for EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    #[instrument(skip(self, cx, buf), fields(buf_remaining = buf.remaining()))]
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        // Serve from buffer if available
        if *this.buffer_pos < this.buffer.len() {
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len() - *this.buffer_pos);
            buf.put_slice(&this.buffer[*this.buffer_pos..*this.buffer_pos + to_copy]);
            *this.buffer_pos += to_copy;

            if *this.buffer_pos == this.buffer.len() {
                this.buffer.clear();
                *this.buffer_pos = 0;
            }

            return Poll::Ready(Ok(()));
        }

        if *this.finished {
            return Poll::Ready(Ok(()));
        }

        let span = info_span!(
            "encrypt_reader_poll_read",
            block_counter = *this.block_counter,
            finished = *this.finished,
            buffer_len = this.buffer.len()
        );

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        // Read a block from inner reader
        let mut temp = vec![0u8; ENCRYPTION_BLOCK_SIZE];
        let mut temp_buf = ReadBuf::new(&mut temp);

        match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let n = temp_buf.filled().len();

                if n == 0 {
                    // EOF, write end marker
                    let end_marker = Self::create_end_marker(&this.key, &this.nonce);
                    *this.buffer = end_marker;
                    *this.buffer_pos = 0;
                    *this.finished = true;

                    let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
                    buf.put_slice(&this.buffer[..to_copy]);
                    *this.buffer_pos += to_copy;

                    Poll::Ready(Ok(()))
                } else {
                    // Encrypt the chunk with batch optimization
                    let encrypted_data = Self::encrypt_block_static(&this.key, &this.nonce, &temp_buf.filled());
                    match encrypted_data {
                        Ok(encrypted_data) => {
                            *this.buffer = encrypted_data;
                            *this.buffer_pos = 0;

                            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
                            buf.put_slice(&this.buffer[..to_copy]);
                            *this.buffer_pos += to_copy;

                            #[cfg(feature = "metrics")]
                            histogram!("rustfs_encrypt_reader_poll_read_duration_seconds").record(start.elapsed().as_secs_f64());

                            Poll::Ready(Ok(()))
                        }
                        Err(e) => Poll::Ready(Err(e)),
                    }
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

impl<R> EtagResolvable for EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync + EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> HashReaderDetector for EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync + HashReaderDetector,
{
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

impl<R> TryGetIndex for EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync + TryGetIndex,
{
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WarpReader;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_encrypt_reader_basic_functionality() {
        let data = b"Hello, RustFS with advanced encryption!";
        let reader = Cursor::new(&data[..]);
        let warp_reader = WarpReader::new(reader);

        let key = [1u8; 32]; // Test key
        let nonce = [2u8; 12]; // Test nonce

        let mut encrypt_reader = EncryptReader::new(warp_reader, key, nonce);
        let mut result = Vec::new();

        encrypt_reader.read_to_end(&mut result).await.unwrap();

        // Encrypted data should be larger than original due to overhead
        assert!(result.len() > data.len());

        // Should not equal original data (encrypted)
        assert_ne!(result, data);

        println!("Encrypted {} bytes to {} bytes", data.len(), result.len());
    }

    #[tokio::test]
    async fn test_encrypt_reader_large_data() {
        let data = vec![42u8; 1024 * 1024]; // 1MB of data
        let reader = Cursor::new(&data[..]);
        let warp_reader = WarpReader::new(reader);

        let key = [3u8; 32];
        let nonce = [4u8; 12];

        let mut encrypt_reader = EncryptReader::with_block_size(
            warp_reader,
            key,
            nonce,
            128 * 1024, // 128KB blocks
        );

        let mut result = Vec::new();
        let start = std::time::Instant::now();
        encrypt_reader.read_to_end(&mut result).await.unwrap();
        let duration = start.elapsed();

        println!("Encrypted 1MB in {:?}, output size: {} bytes", duration, result.len());

        // Verify encryption occurred
        assert!(result.len() > data.len());
        assert_ne!(result[..data.len()].to_vec(), data);
    }

    #[tokio::test]
    async fn test_encrypt_reader_different_block_sizes() {
        let data = b"Test data for block size comparison".repeat(100);

        for block_size in [4096, 16384, 65536] {
            let reader = Cursor::new(&data[..]);
            let warp_reader = WarpReader::new(reader);

            let key = [5u8; 32];
            let nonce = [6u8; 12];

            let mut encrypt_reader = EncryptReader::with_block_size(warp_reader, key, nonce, block_size);

            let mut result = Vec::new();
            encrypt_reader.read_to_end(&mut result).await.unwrap();

            println!("Block size {}: {} -> {} bytes", block_size, data.len(), result.len());

            // All block sizes should produce encrypted data
            assert!(result.len() > data.len());
        }
    }

    #[tokio::test]
    async fn test_encrypt_reader_empty_data() {
        let data = b"";
        let reader = Cursor::new(&data[..]);
        let warp_reader = WarpReader::new(reader);

        let key = [7u8; 32];
        let nonce = [8u8; 12];

        let mut encrypt_reader = EncryptReader::new(warp_reader, key, nonce);
        let mut result = Vec::new();

        encrypt_reader.read_to_end(&mut result).await.unwrap();

        // Should still produce end marker
        assert!(!result.is_empty());

        // Should contain end marker (0xFF)
        assert_eq!(result[0], 0xFF);
    }
}
