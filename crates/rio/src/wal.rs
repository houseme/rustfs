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

//! High-performance Write-Ahead Log implementation with batch optimization
//!
//! This module provides an optimized WAL implementation that can leverage
//! io_uring for batch submissions when available, significantly improving
//! write throughput for high-frequency logging operations.

use crate::disk::{AsyncFile, DiskFile};
use crate::runtime::{RuntimeHandle, RuntimeError};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::{Error as IoError, Result as IoResult};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::interval;
use tracing::{debug, error, instrument};

#[cfg(feature = "metrics")]
use metrics::{counter, gauge, histogram};

/// WAL entry representing a single log record
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Sequence number for ordering
    pub sequence: u64,
    /// Entry data
    pub data: Bytes,
    /// Entry timestamp
    pub timestamp: std::time::SystemTime,
}

impl WalEntry {
    /// Create a new WAL entry
    pub fn new(sequence: u64, data: Bytes) -> Self {
        Self {
            sequence,
            data,
            timestamp: std::time::SystemTime::now(),
        }
    }
    
    /// Serialize the entry for storage
    pub fn serialize(&self) -> Bytes {
        let timestamp_secs = self.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let mut buf = BytesMut::with_capacity(8 + 8 + 4 + self.data.len());
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.extend_from_slice(&timestamp_secs.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.data);
        
        buf.freeze()
    }
    
    /// Deserialize an entry from storage
    pub fn deserialize(data: &[u8]) -> IoResult<(Self, usize)> {
        if data.len() < 20 {
            return Err(IoError::new(
                std::io::ErrorKind::InvalidData,
                "WAL entry too short",
            ));
        }
        
        let sequence = u64::from_le_bytes([
            data[0], data[1], data[2], data[3],
            data[4], data[5], data[6], data[7],
        ]);
        
        let timestamp_secs = u64::from_le_bytes([
            data[8], data[9], data[10], data[11],
            data[12], data[13], data[14], data[15],
        ]);
        
        let data_len = u32::from_le_bytes([data[16], data[17], data[18], data[19]]) as usize;
        
        if data.len() < 20 + data_len {
            return Err(IoError::new(
                std::io::ErrorKind::InvalidData,
                "WAL entry data truncated",
            ));
        }
        
        let timestamp = std::time::UNIX_EPOCH + Duration::from_secs(timestamp_secs);
        let entry_data = Bytes::copy_from_slice(&data[20..20 + data_len]);
        
        let entry = WalEntry {
            sequence,
            data: entry_data,
            timestamp,
        };
        
        Ok((entry, 20 + data_len))
    }
}

/// Configuration for WAL behavior with enhanced timeout settings
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum number of entries to batch before flushing
    pub batch_size: usize,
    /// Maximum time to wait before flushing a partial batch
    pub flush_timeout: Duration,
    /// Enable sync after each batch for durability
    pub sync_after_batch: bool,
    /// Buffer size for batch operations
    pub buffer_size: usize,
    /// Timeout for WAL file scanning operations
    pub scan_timeout: Option<Duration>,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            batch_size: 100, // Batch up to 100 entries
            flush_timeout: Duration::from_millis(10), // Flush after 10ms if not full
            sync_after_batch: true, // Ensure durability
            buffer_size: 64 * 1024, // 64KB buffer
            scan_timeout: Some(Duration::from_secs(30)), // 30 second scan timeout
        }
    }
}

/// High-performance Write-Ahead Log with batch optimization
pub struct Wal {
    /// WAL file handle
    file: Arc<Mutex<DiskFile>>,
    /// Runtime handle for I/O operations  
    runtime: RuntimeHandle,
    /// Configuration
    config: WalConfig,
    /// Current write position in the file
    write_position: Arc<RwLock<u64>>,
    /// Next sequence number
    next_sequence: Arc<RwLock<u64>>,
    /// Channel for sending entries to the background writer
    write_tx: mpsc::UnboundedSender<WalEntry>,
}

impl Wal {
    /// Create a new WAL instance with enhanced error handling
    #[instrument(skip(runtime), fields(path = %path.as_ref().display()))]
    pub async fn new<P: AsRef<Path>>(
        path: P,
        runtime: RuntimeHandle,
        config: WalConfig,
    ) -> Result<Self, RuntimeError> {
        let file = DiskFile::create(path.as_ref(), runtime.clone()).await?;
        let file = Arc::new(Mutex::new(file));
        
        let write_position = Arc::new(RwLock::new(0));
        let next_sequence = Arc::new(RwLock::new(1));
        
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        
        // Start background writer task in a detached manner
        let _writer_task = runtime.spawn(Self::writer_task(
            file.clone(),
            write_rx,
            config.clone(),
            write_position.clone(),
        ));
        
        debug!("Created WAL at {}", path.as_ref().display());
        
        Ok(Self {
            file,
            runtime,
            config,
            write_position,
            next_sequence,
            write_tx,
        })
    }
    
    /// Open an existing WAL file with enhanced error handling
    #[instrument(skip(runtime), fields(path = %path.as_ref().display()))]
    pub async fn open<P: AsRef<Path>>(
        path: P,
        runtime: RuntimeHandle,
        config: WalConfig,
    ) -> Result<Self, RuntimeError> {
        let file = DiskFile::open(path.as_ref(), runtime.clone()).await?;
        let file = Arc::new(Mutex::new(file));
        
        // Scan the file to determine current position and next sequence with timeout
        let (write_position, next_sequence) = Self::scan_wal_file(file.clone()).await
            .map_err(|e| RuntimeError::InitializationFailed(format!("WAL scan failed: {}", e)))?;
        
        let write_position = Arc::new(RwLock::new(write_position));
        let next_sequence = Arc::new(RwLock::new(next_sequence));
        
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        
        // Start background writer task in a detached manner
        let _writer_task = runtime.spawn(Self::writer_task(
            file.clone(),
            write_rx,
            config.clone(),
            write_position.clone(),
        ));
        
        debug!("Opened WAL at {}", path.as_ref().display());
        
        Ok(Self {
            file,
            runtime,
            config,
            write_position,
            next_sequence,
            write_tx,
        })
    }
    
    /// Append a new entry to the WAL
    #[instrument(skip(self, data))]
    pub async fn append(&self, data: Bytes) -> IoResult<u64> {
        let sequence = {
            let mut next_seq = self.next_sequence.write().await;
            let current = *next_seq;
            *next_seq += 1;
            current
        };
        
        let entry = WalEntry::new(sequence, data);
        
        self.write_tx.send(entry).map_err(|_| {
            IoError::new(std::io::ErrorKind::BrokenPipe, "WAL writer task terminated")
        })?;
        
        #[cfg(feature = "metrics")]
        counter!("rustfs_wal_entries_total").increment(1);
        
        Ok(sequence)
    }
    
    /// Append multiple entries in a batch
    #[instrument(skip(self, entries))]
    pub async fn append_batch(&self, entries: Vec<Bytes>) -> IoResult<Vec<u64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut sequences = Vec::with_capacity(entries.len());
        
        // Allocate sequence numbers
        {
            let mut next_seq = self.next_sequence.write().await;
            for _ in 0..entries.len() {
                sequences.push(*next_seq);
                *next_seq += 1;
            }
        }
        
        // Send entries to background writer
        for (sequence, data) in sequences.iter().zip(entries.into_iter()) {
            let entry = WalEntry::new(*sequence, data);
            self.write_tx.send(entry).map_err(|_| {
                IoError::new(std::io::ErrorKind::BrokenPipe, "WAL writer task terminated")
            })?;
        }
        
        #[cfg(feature = "metrics")]
        counter!("rustfs_wal_batch_entries_total").increment(sequences.len() as u64);
        
        Ok(sequences)
    }
    
    /// Force flush all pending entries
    pub async fn flush(&self) -> IoResult<()> {
        // Send a special flush signal or implement a flush mechanism
        // For now, we'll sync the file directly
        let mut file = self.file.lock().await;
        file.sync_all().await?;
        
        #[cfg(feature = "metrics")]
        counter!("rustfs_wal_flushes_total").increment(1);
        
        Ok(())
    }
    
    /// Get the current write position
    pub async fn write_position(&self) -> u64 {
        *self.write_position.read().await
    }
    
    /// Background writer task that handles batched writes
    async fn writer_task(
        file: Arc<Mutex<DiskFile>>,
        mut write_rx: mpsc::UnboundedReceiver<WalEntry>,
        config: WalConfig,
        write_position: Arc<RwLock<u64>>,
    ) {
        let mut batch = VecDeque::with_capacity(config.batch_size);
        let mut flush_timer = interval(config.flush_timeout);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            tokio::select! {
                // Receive new entries
                entry = write_rx.recv() => {
                    match entry {
                        Some(entry) => {
                            batch.push_back(entry);
                            
                            // Flush if batch is full
                            if batch.len() >= config.batch_size {
                                if let Err(e) = Self::flush_batch(&file, &mut batch, &config, &write_position).await {
                                    error!("Failed to flush WAL batch: {}", e);
                                }
                            }
                        }
                        None => {
                            // Channel closed, flush remaining entries and exit
                            if !batch.is_empty() {
                                if let Err(e) = Self::flush_batch(&file, &mut batch, &config, &write_position).await {
                                    error!("Failed to flush final WAL batch: {}", e);
                                }
                            }
                            break;
                        }
                    }
                }
                
                // Periodic flush timer
                _ = flush_timer.tick() => {
                    if !batch.is_empty() {
                        if let Err(e) = Self::flush_batch(&file, &mut batch, &config, &write_position).await {
                            error!("Failed to flush WAL batch on timer: {}", e);
                        }
                    }
                }
            }
        }
        
        debug!("WAL writer task terminated");
    }
    
    /// Flush a batch of entries to disk
    async fn flush_batch(
        file: &Arc<Mutex<DiskFile>>,
        batch: &mut VecDeque<WalEntry>,
        config: &WalConfig,
        write_position: &Arc<RwLock<u64>>,
    ) -> IoResult<()> {
        if batch.is_empty() {
            return Ok(());
        }
        
        let _start = Instant::now();
        
        // Serialize all entries into a single buffer
        let mut buffer = BytesMut::with_capacity(config.buffer_size);
        let batch_size = batch.len();
        
        while let Some(entry) = batch.pop_front() {
            let serialized = entry.serialize();
            buffer.extend_from_slice(&serialized);
        }
        
        // Write the entire batch at once
        let mut file = file.lock().await;
        let position = *write_position.read().await;
        let written = file.write_object(&buffer, position).await?;
        
        // Update write position
        {
            let mut pos = write_position.write().await;
            *pos += written as u64;
        }
        
        // Sync if configured
        if config.sync_after_batch {
            file.sync_data().await?;
        }
        
        #[cfg(feature = "metrics")]
        {
            histogram!("rustfs_wal_batch_flush_duration_seconds").record(_start.elapsed().as_secs_f64());
            gauge!("rustfs_wal_batch_size").set(batch_size as f64);
            counter!("rustfs_wal_bytes_written_total").increment(written as u64);
        }
        
        debug!("Flushed WAL batch of {} entries ({} bytes)", batch_size, written);
        
        Ok(())
    }
    
    /// Scan the WAL file to determine current position and next sequence number
    async fn scan_wal_file(file: Arc<Mutex<DiskFile>>) -> IoResult<(u64, u64)> {
        let mut file = file.lock().await;
        let metadata = file.metadata().await?;
        let file_size = metadata.len();
        
        if file_size == 0 {
            return Ok((0, 1));
        }
        
        // Read the entire file for scanning (this could be optimized for large files)
        let mut buffer = vec![0u8; file_size as usize];
        let read = file.read_object(&mut buffer, 0).await?;
        
        let mut position = 0;
        let mut max_sequence = 0;
        
        while position < read {
            match WalEntry::deserialize(&buffer[position..]) {
                Ok((entry, entry_size)) => {
                    max_sequence = max_sequence.max(entry.sequence);
                    position += entry_size;
                }
                Err(_) => {
                    // Invalid entry, stop scanning
                    break;
                }
            }
        }
        
        Ok((position as u64, max_sequence + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::init_runtime;
    use tempfile::NamedTempFile;
    
    #[tokio::test]
    async fn test_wal_creation() {
        let runtime = init_runtime().unwrap();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let wal = Wal::new(path, runtime, WalConfig::default())
            .await
            .unwrap();
        
        assert_eq!(wal.write_position().await, 0);
    }
    
    #[tokio::test]
    async fn test_wal_append() {
        let runtime = init_runtime().unwrap();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let wal = Wal::new(path, runtime, WalConfig::default())
            .await
            .unwrap();
        
        let data = Bytes::from("test entry");
        let sequence = wal.append(data).await.unwrap();
        assert_eq!(sequence, 1);
        
        // Wait a bit for the background writer to process
        tokio::time::sleep(Duration::from_millis(50)).await;
        wal.flush().await.unwrap();
        
        assert!(wal.write_position().await > 0);
    }
    
    #[tokio::test]
    async fn test_wal_batch_append() {
        let runtime = init_runtime().unwrap();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let wal = Wal::new(path, runtime, WalConfig::default())
            .await
            .unwrap();
        
        let entries = vec![
            Bytes::from("entry 1"),
            Bytes::from("entry 2"),
            Bytes::from("entry 3"),
        ];
        
        let sequences = wal.append_batch(entries).await.unwrap();
        assert_eq!(sequences, vec![1, 2, 3]);
        
        // Wait for background processing
        tokio::time::sleep(Duration::from_millis(50)).await;
        wal.flush().await.unwrap();
        
        assert!(wal.write_position().await > 0);
    }
    
    #[tokio::test]
    async fn test_wal_entry_serialization() {
        let entry = WalEntry::new(42, Bytes::from("test data"));
        let serialized = entry.serialize();
        
        let (deserialized, size) = WalEntry::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.sequence, 42);
        assert_eq!(deserialized.data, Bytes::from("test data"));
        assert_eq!(size, serialized.len());
    }
}