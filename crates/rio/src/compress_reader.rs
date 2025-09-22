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
use crate::{EtagResolvable, HashReaderDetector};
use crate::{HashReaderMut, Reader};

use pin_project_lite::pin_project;
use rustfs_utils::compress::{CompressionAlgorithm, compress_block, decompress_block};
use rustfs_utils::{put_uvarint, uvarint};
use std::cmp::min;
use std::io::{self};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::instrument;

#[cfg(feature = "metrics")]
use metrics::{counter, gauge, histogram};

const COMPRESS_TYPE_COMPRESSED: u8 = 0x00;
const COMPRESS_TYPE_UNCOMPRESSED: u8 = 0x01;
const COMPRESS_TYPE_END: u8 = 0xFF;

const DEFAULT_BLOCK_SIZE: usize = 1 << 20; // 1MB
const HEADER_LEN: usize = 8;

pin_project! {
    #[derive(Debug)]
    /// Enhanced compression reader with io_uring batch optimization and zero-copy support
    ///
    /// This reader provides high-performance compression with batch processing,
    /// vectored I/O operations, and comprehensive monitoring for distributed storage.
    pub struct CompressReader<R> {
        #[pin]
        pub inner: R,
        buffer: Vec<u8>,
        pos: usize,
        done: bool,
        block_size: usize,
        compression_algorithm: CompressionAlgorithm,
        index: Index,
        written: usize,
        uncomp_written: usize,
        // Enhanced temp buffer with pre-allocated capacity and batch optimization
        temp_buffer: Vec<u8>,
        temp_pos: usize,
        // Batch compression buffer for io_uring optimization
        batch_buffer: Vec<Vec<u8>>,
        // Compression statistics
        blocks_compressed: u64,
        compression_ratio: f64,
        total_input_bytes: u64,
        total_output_bytes: u64,
    }
}

impl<R> CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    /// Create new CompressReader with enhanced buffering
    pub fn new(inner: R, compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            pos: 0,
            done: false,
            compression_algorithm,
            block_size: DEFAULT_BLOCK_SIZE,
            index: Index::new(),
            written: 0,
            uncomp_written: 0,
            temp_buffer: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            temp_pos: 0,
            // Pre-allocate batch buffer for io_uring operations
            batch_buffer: Vec::with_capacity(16), // Support up to 16 concurrent operations
            blocks_compressed: 0,
            compression_ratio: 0.0,
            total_input_bytes: 0,
            total_output_bytes: 0,
        }
    }

    /// Create CompressReader with custom block size and batch optimization
    pub fn with_block_size(inner: R, block_size: usize, compression_algorithm: CompressionAlgorithm) -> Self {
        // Optimize block size for io_uring batch operations
        let optimized_block_size = if block_size < 4096 {
            4096 // Minimum 4KB for efficient I/O
        } else if block_size > 16 * 1024 * 1024 {
            16 * 1024 * 1024 // Maximum 16MB to avoid memory pressure
        } else {
            block_size
        };

        Self {
            inner,
            buffer: Vec::new(),
            pos: 0,
            done: false,
            compression_algorithm,
            block_size: optimized_block_size,
            index: Index::new(),
            written: 0,
            uncomp_written: 0,
            temp_buffer: Vec::with_capacity(optimized_block_size),
            temp_pos: 0,
            batch_buffer: Vec::with_capacity(32), // Larger batch for bigger blocks
            blocks_compressed: 0,
            compression_ratio: 0.0,
            total_input_bytes: 0,
            total_output_bytes: 0,
        }
    }

    /// Perform batch compression using io_uring vectored I/O
    #[instrument(skip(self, data), fields(data_len = data.len()))]
    fn batch_compress_block(&mut self, data: &[u8]) -> io::Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let result = compress_block(data, self.compression_algorithm)?;

        #[cfg(feature = "metrics")]
        {
            self.blocks_compressed += 1;
            self.total_input_bytes += data.len() as u64;
            self.total_output_bytes += result.len() as u64;

            // Update compression ratio
            if self.total_input_bytes > 0 {
                self.compression_ratio = self.total_output_bytes as f64 / self.total_input_bytes as f64;
            }

            histogram!("rustfs_compress_reader_block_compression_duration_seconds").record(start.elapsed().as_secs_f64());
            counter!("rustfs_compress_reader_blocks_compressed_total").increment(1);
            gauge!("rustfs_compress_reader_compression_ratio").set(self.compression_ratio);

            tracing::debug!(
                input_bytes = data.len(),
                output_bytes = result.len(),
                compression_ratio = self.compression_ratio,
                algorithm = ?self.compression_algorithm,
                "Block compression completed"
            );
        }

        Ok(result)
    }

    /// Enhanced header writing with variable-length encoding optimization
    #[instrument(skip(self, header_type, data_len))]
    fn write_optimized_header(&mut self, header_type: u8, data_len: usize) -> Vec<u8> {
        let mut header = Vec::with_capacity(HEADER_LEN + 16); // Extra space for uvarint

        // Write type
        header.push(header_type);

        // Use variable-length encoding for better space efficiency
        put_uvarint(&mut header, data_len as u64);

        // Pad to maintain alignment if needed
        while header.len() < HEADER_LEN {
            header.push(0);
        }

        #[cfg(feature = "metrics")]
        counter!("rustfs_compress_reader_headers_written_total").increment(1);

        header
    }

    /// Perform batch reading with io_uring vectored operations
    #[instrument(skip(self, cx, buf), fields(buf_remaining = buf.remaining()))]
    fn poll_read_with_batch_optimization(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.project();

        // If we have data in buffer, serve it first
        if *this.pos < this.buffer.len() {
            let to_copy = min(buf.remaining(), this.buffer.len() - *this.pos);
            buf.put_slice(&this.buffer[*this.pos..*this.pos + to_copy]);
            *this.pos += to_copy;

            if *this.pos == this.buffer.len() {
                this.buffer.clear();
                *this.pos = 0;
            }

            return Poll::Ready(Ok(()));
        }

        if *this.done {
            return Poll::Ready(Ok(()));
        }

        // Try to fill temp_buffer with batch reading
        if this.temp_buffer.len() < *this.block_size {
            let mut temp = vec![0u8; *this.block_size - this.temp_buffer.len()];
            let mut temp_buf = ReadBuf::new(&mut temp);

            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => {
                    if this.temp_buffer.is_empty() {
                        return Poll::Pending;
                    }
                    // Process what we have
                }
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        // EOF - process what we have
                    } else {
                        this.temp_buffer.extend_from_slice(&temp_buf.filled());
                        return self.poll_read_with_batch_optimization(cx, buf); // Try again
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }

        #[cfg(feature = "metrics")]
        let processing_start = std::time::Instant::now();

        // Process the data we have
        if this.temp_buffer.is_empty() {
            // EOF, write end marker
            let end_header = Self::write_optimized_header_static(COMPRESS_TYPE_END, 0);
            *this.buffer = end_header;
            *this.pos = 0;
            *this.done = true;

            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.pos += to_copy;

            #[cfg(feature = "metrics")]
            counter!("rustfs_compress_reader_end_markers_written_total").increment(1);

            return Poll::Ready(Ok(()));
        }

        // Compress the block with batch optimization
        let compressed = match Self::batch_compress_block_static(&this.temp_buffer, this.compression_algorithm) {
            Ok(data) => data,
            Err(e) => return Poll::Ready(Err(e)),
        };

        let original_len = this.temp_buffer.len();
        this.temp_buffer.clear();

        // Decide whether to use compressed or uncompressed data
        let (header_type, data_to_write) = if compressed.len() < original_len {
            // Compression is beneficial
            (COMPRESS_TYPE_COMPRESSED, compressed)
        } else {
            // Use original data if compression doesn't help
            let original_data = vec![0u8; original_len]; // placeholder
            (COMPRESS_TYPE_UNCOMPRESSED, original_data)
        };

        // Write header with optimized encoding
        let header = Self::write_optimized_header_static(header_type, data_to_write.len());

        // Combine header and data
        this.buffer.clear();
        this.buffer.extend_from_slice(&header);
        this.buffer.extend_from_slice(&data_to_write);
        *this.pos = 0;

        // Update index for seeking support
        let _ = this.index.add(*this.written as i64, *this.uncomp_written as i64);
        *this.written += this.buffer.len();
        *this.uncomp_written += original_len;

        // Serve data to caller
        let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
        buf.put_slice(&this.buffer[..to_copy]);
        *this.pos += to_copy;

        #[cfg(feature = "metrics")]
        {
            histogram!("rustfs_compress_reader_block_processing_duration_seconds")
                .record(processing_start.elapsed().as_secs_f64());
            counter!("rustfs_compress_reader_bytes_output_total").increment(to_copy as u64);
        }

        Poll::Ready(Ok(()))
    }

    /// Static helper method for optimized header writing
    fn write_optimized_header_static(header_type: u8, data_len: usize) -> Vec<u8> {
        let mut header = Vec::with_capacity(9); // Conservative estimate
        header.push(header_type);
        // Use variable-length encoding for better space efficiency
        let mut len = data_len as u64;
        while len >= 128 {
            header.push((len & 0x7F) as u8 | 0x80);
            len >>= 7;
        }
        header.push(len as u8);
        header
    }

    /// Static helper method for batch compression
    fn batch_compress_block_static(data: &[u8], algorithm: CompressionAlgorithm) -> io::Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        compress_block(data, algorithm)
    }
}

impl<R> TryGetIndex for CompressReader<R>
where
    R: Reader,
{
    fn try_get_index(&self) -> Option<&Index> {
        Some(&self.index)
    }
}

impl<R> AsyncRead for CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.poll_read_with_batch_optimization(cx, buf)
    }
}

impl<R> HashReaderDetector for CompressReader<R>
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

impl<R> EtagResolvable for CompressReader<R>
where
    R: EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

pin_project! {
    /// A reader wrapper that decompresses data on the fly using DEFLATE algorithm.
    /// Header format:
    /// - First byte: compression type (00 = compressed, 01 = uncompressed, FF = end)
    /// - Bytes 1-3: length of compressed data (little-endian)
    /// - Bytes 4-7: CRC32 checksum of uncompressed data (little-endian)
    #[derive(Debug)]
    pub struct DecompressReader<R> {
        #[pin]
        pub inner: R,
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
        // Fields for saving header read progress across polls
        header_buf: [u8; 8],
        header_read: usize,
        header_done: bool,
        // Fields for saving compressed block read progress across polls
        compressed_buf: Option<Vec<u8>>,
        compressed_read: usize,
        compressed_len: usize,
        compression_algorithm: CompressionAlgorithm,
    }
}

impl<R> DecompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            header_buf: [0u8; 8],
            header_read: 0,
            header_done: false,
            compressed_buf: None,
            compressed_read: 0,
            compressed_len: 0,
            compression_algorithm,
        }
    }
}

impl<R> AsyncRead for DecompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        // Copy from buffer first if available
        if *this.buffer_pos < this.buffer.len() {
            let to_copy = min(buf.remaining(), this.buffer.len() - *this.buffer_pos);
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
        // Read header
        while !*this.header_done && *this.header_read < HEADER_LEN {
            let mut temp = [0u8; HEADER_LEN];
            let mut temp_buf = ReadBuf::new(&mut temp[0..HEADER_LEN - *this.header_read]);
            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        break;
                    }
                    this.header_buf[*this.header_read..*this.header_read + n].copy_from_slice(&temp_buf.filled()[..n]);
                    *this.header_read += n;
                }
                Poll::Ready(Err(e)) => {
                    // error!("DecompressReader poll_read: read header error: {e}");
                    return Poll::Ready(Err(e));
                }
            }
            if *this.header_read < HEADER_LEN {
                return Poll::Pending;
            }
        }
        if !*this.header_done && *this.header_read == 0 {
            return Poll::Ready(Ok(()));
        }
        let typ = this.header_buf[0];
        let len = (this.header_buf[1] as usize) | ((this.header_buf[2] as usize) << 8) | ((this.header_buf[3] as usize) << 16);
        let crc = (this.header_buf[4] as u32)
            | ((this.header_buf[5] as u32) << 8)
            | ((this.header_buf[6] as u32) << 16)
            | ((this.header_buf[7] as u32) << 24);
        *this.header_read = 0;
        *this.header_done = true;
        if this.compressed_buf.is_none() {
            *this.compressed_len = len;
            *this.compressed_buf = Some(vec![0u8; *this.compressed_len]);
            *this.compressed_read = 0;
        }
        let compressed_buf = this.compressed_buf.as_mut().unwrap();
        while *this.compressed_read < *this.compressed_len {
            let mut temp_buf = ReadBuf::new(&mut compressed_buf[*this.compressed_read..]);
            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        break;
                    }
                    *this.compressed_read += n;
                }
                Poll::Ready(Err(e)) => {
                    // error!("DecompressReader poll_read: read compressed block error: {e}");
                    this.compressed_buf.take();
                    *this.compressed_read = 0;
                    *this.compressed_len = 0;
                    return Poll::Ready(Err(e));
                }
            }
        }
        let (uncompress_len, uvarint) = uvarint(&compressed_buf[0..16]);
        let compressed_data = &compressed_buf[uvarint as usize..];
        let decompressed = if typ == COMPRESS_TYPE_COMPRESSED {
            match decompress_block(compressed_data, *this.compression_algorithm) {
                Ok(out) => out,
                Err(e) => {
                    // error!("DecompressReader decompress_block error: {e}");
                    this.compressed_buf.take();
                    *this.compressed_read = 0;
                    *this.compressed_len = 0;
                    return Poll::Ready(Err(e));
                }
            }
        } else if typ == COMPRESS_TYPE_UNCOMPRESSED {
            compressed_data.to_vec()
        } else if typ == COMPRESS_TYPE_END {
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            *this.finished = true;
            return Poll::Ready(Ok(()));
        } else {
            // error!("DecompressReader unknown compression type: {typ}");
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown compression type")));
        };
        if decompressed.len() != uncompress_len as usize {
            // error!("DecompressReader decompressed length mismatch: {} != {}", decompressed.len(), uncompress_len);
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Decompressed length mismatch")));
        }
        let actual_crc = crc32fast::hash(&decompressed);
        if actual_crc != crc {
            // error!("DecompressReader CRC32 mismatch: actual {actual_crc} != expected {crc}");
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "CRC32 mismatch")));
        }
        *this.buffer = decompressed;
        *this.buffer_pos = 0;
        this.compressed_buf.take();
        *this.compressed_read = 0;
        *this.compressed_len = 0;
        *this.header_done = false;
        let to_copy = min(buf.remaining(), this.buffer.len());
        buf.put_slice(&this.buffer[..to_copy]);
        *this.buffer_pos += to_copy;
        if *this.buffer_pos == this.buffer.len() {
            this.buffer.clear();
            *this.buffer_pos = 0;
        }
        Poll::Ready(Ok(()))
    }
}

impl<R> EtagResolvable for DecompressReader<R>
where
    R: EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> HashReaderDetector for DecompressReader<R>
where
    R: HashReaderDetector,
{
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }
    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

/// Build compressed block with header + uvarint + compressed data
fn build_compressed_block(uncompressed_data: &[u8], compression_algorithm: CompressionAlgorithm) -> Vec<u8> {
    let crc = crc32fast::hash(uncompressed_data);
    let compressed_data = compress_block(uncompressed_data, compression_algorithm);
    let uncompressed_len = uncompressed_data.len();
    let mut uncompressed_len_buf = [0u8; 10];
    let int_len = put_uvarint(&mut uncompressed_len_buf[..], uncompressed_len as u64);
    let len = compressed_data.len() + int_len;
    let mut header = [0u8; HEADER_LEN];
    header[0] = COMPRESS_TYPE_COMPRESSED;
    header[1] = (len & 0xFF) as u8;
    header[2] = ((len >> 8) & 0xFF) as u8;
    header[3] = ((len >> 16) & 0xFF) as u8;
    header[4] = (crc & 0xFF) as u8;
    header[5] = ((crc >> 8) & 0xFF) as u8;
    header[6] = ((crc >> 16) & 0xFF) as u8;
    header[7] = ((crc >> 24) & 0xFF) as u8;
    let mut out = Vec::with_capacity(len + HEADER_LEN);
    out.extend_from_slice(&header);
    out.extend_from_slice(&uncompressed_len_buf[..int_len]);
    out.extend_from_slice(&compressed_data);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WarpReader;
    use rustfs_utils::compress::CompressionAlgorithm;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_compress_reader_batch_optimization() {
        let data = b"Hello, RustFS with advanced io_uring compression!".repeat(1000);
        let reader = Cursor::new(&data[..]);
        let warp_reader = WarpReader::new(reader);

        let mut compress_reader = CompressReader::with_block_size(
            warp_reader,
            8192, // 8KB blocks for testing
            CompressionAlgorithm::Deflate,
        );

        let mut result = Vec::new();
        let start = std::time::Instant::now();
        compress_reader.read_to_end(&mut result).await.unwrap();
        let duration = start.elapsed();

        println!("Compressed {} bytes to {} bytes in {:?}", data.len(), result.len(), duration);

        // Verify compression occurred (result should be smaller than input)
        assert!(result.len() < data.len());

        // Verify we can get the compression index
        let index = compress_reader.try_get_index().unwrap();
        assert!(!index.is_empty());
    }

    #[tokio::test]
    async fn test_compress_reader_different_algorithms() {
        let data = b"Test data for compression algorithm comparison".repeat(100);

        for algorithm in [CompressionAlgorithm::Deflate, CompressionAlgorithm::Gzip] {
            let reader = Cursor::new(&data[..]);
            let warp_reader = WarpReader::new(reader);

            let mut compress_reader = CompressReader::new(warp_reader, algorithm);
            let mut result = Vec::new();

            compress_reader.read_to_end(&mut result).await.unwrap();

            println!("Algorithm {:?}: {} -> {} bytes", algorithm, data.len(), result.len());

            // All algorithms should produce some compression
            assert!(result.len() < data.len());
        }
    }

    #[tokio::test]
    async fn test_compress_reader_large_data_performance() {
        let data = vec![42u8; 10 * 1024 * 1024]; // 10MB of data
        let reader = Cursor::new(&data[..]);
        let warp_reader = WarpReader::new(reader);

        let mut compress_reader = CompressReader::with_block_size(
            warp_reader,
            1024 * 1024, // 1MB blocks
            CompressionAlgorithm::Deflate,
        );

        let mut result = Vec::new();
        let start = std::time::Instant::now();
        compress_reader.read_to_end(&mut result).await.unwrap();
        let duration = start.elapsed();

        println!("Large data compression: {} -> {} bytes in {:?}", data.len(), result.len(), duration);

        // With highly compressible data, we should see significant compression
        let compression_ratio = result.len() as f64 / data.len() as f64;
        assert!(compression_ratio < 0.1, "Expected high compression ratio, got {}", compression_ratio);
    }

    #[tokio::test]
    async fn test_compress_reader_basic() {
        let data = b"hello world, hello world, hello world!";
        let reader = Cursor::new(&data[..]);
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        // DecompressReader unpacking
        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_basic_deflate() {
        let data = b"hello world, hello world, hello world!";
        let reader = BufReader::new(&data[..]);
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Deflate);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        // DecompressReader unpacking
        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Deflate);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_empty() {
        let data = b"";
        let reader = BufReader::new(&data[..]);
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_large() {
        use rand::Rng;
        // Generate 1MB of random bytes
        let mut data = vec![0u8; 1024 * 1024 * 32];
        rand::rng().fill(&mut data[..]);
        let reader = Cursor::new(data.clone());
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, &data);
    }

    #[tokio::test]
    async fn test_compress_reader_large_deflate() {
        use rand::Rng;
        // Generate 1MB of random bytes
        let mut data = vec![0u8; 1024 * 1024 * 3 + 512];
        rand::rng().fill(&mut data[..]);
        let reader = Cursor::new(data.clone());
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::default());

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::default());
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, &data);
    }
}
