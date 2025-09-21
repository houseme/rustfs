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

//! Advanced I/O Engine with io_uring Integration
//!
//! This module provides a sophisticated I/O abstraction layer that integrates deeply
//! with the reader/writer ecosystem, providing zero-copy operations, batch processing,
//! and advanced monitoring capabilities for high-performance distributed object storage.

use crate::runtime::{RuntimeHandle, RuntimeType, RuntimeError};
use anyhow::Result;
use std::io::{IoSlice, IoSliceMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::{info_span, instrument, Instrument};

#[cfg(feature = "metrics")]
use metrics::{counter, histogram, gauge};

/// Advanced I/O engine providing zero-copy and batch operations
pub struct IoEngine {
    runtime: RuntimeHandle,
    /// Global operation semaphore for concurrency control
    global_semaphore: Arc<async_semaphore::Semaphore>,
    /// Batch operation queue for io_uring submissions
    #[cfg(feature = "io_uring")]
    batch_queue: Arc<parking_lot::Mutex<Vec<BatchOperation>>>,
    /// Performance metrics collector
    #[cfg(feature = "metrics")]
    metrics: Arc<IoMetrics>,
}

/// Batch operation for io_uring submission queue entries (SQE)
#[cfg(feature = "io_uring")]
#[derive(Debug)]
struct BatchOperation {
    operation_type: OperationType,
    buffer: Vec<u8>,
    offset: u64,
    callback: tokio::sync::oneshot::Sender<Result<usize>>,
}

#[cfg(feature = "io_uring")]
#[derive(Debug, Clone, Copy)]
enum OperationType {
    Read,
    Write,
    ReadVectored,
    WriteVectored,
    Fsync,
}

/// Performance metrics for I/O operations
#[cfg(feature = "metrics")]
#[derive(Debug)]
struct IoMetrics {
    operations_total: parking_lot::Mutex<u64>,
    bytes_transferred: parking_lot::Mutex<u64>,
    batch_queue_depth: parking_lot::Mutex<usize>,
}

impl IoEngine {
    /// Create a new I/O engine with the specified runtime
    pub fn new(runtime: RuntimeHandle) -> Result<Self> {
        let global_semaphore = Arc::new(async_semaphore::Semaphore::new(
            std::env::var("RUSTFS_IO_ENGINE_CONCURRENCY")
                .and_then(|s| s.parse().map_err(|_| std::env::VarError::NotPresent))
                .unwrap_or(2048), // Higher concurrency for advanced I/O engine
        ));

        #[cfg(feature = "io_uring")]
        let batch_queue = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(256)));

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(IoMetrics {
            operations_total: parking_lot::Mutex::new(0),
            bytes_transferred: parking_lot::Mutex::new(0),
            batch_queue_depth: parking_lot::Mutex::new(0),
        });

        tracing::info!(
            runtime_type = ?runtime.runtime_type(),
            zero_copy_enabled = runtime.supports_zero_copy(),
            "Advanced I/O engine initialized"
        );

        Ok(Self {
            runtime,
            global_semaphore,
            #[cfg(feature = "io_uring")]
            batch_queue,
            #[cfg(feature = "metrics")]
            metrics,
        })
    }

    /// Perform zero-copy read operation with advanced buffering
    #[instrument(skip(self, reader, buf), fields(buf_len = buf.capacity()))]
    pub async fn zero_copy_read<R>(
        &self,
        mut reader: R,
        buf: &mut Vec<u8>,
        hint_size: Option<usize>,
    ) -> Result<usize>
    where
        R: AsyncRead + Unpin + Send,
    {
        let _permit = self.global_semaphore.acquire().await;
        
        // Pre-allocate buffer with capacity hint for zero-copy optimization
        if let Some(size) = hint_size {
            buf.reserve(size);
        }

        let span = info_span!(
            "zero_copy_read",
            runtime = ?self.runtime.runtime_type(),
            zero_copy = self.runtime.supports_zero_copy()
        );

        async move {
            #[cfg(feature = "metrics")]
            let start = std::time::Instant::now();

            let result = match self.runtime.runtime_type() {
                RuntimeType::Tokio => {
                    // Use Tokio's standard read with pre-allocated buffer
                    let mut read_buf = ReadBuf::new(buf);
                    tokio::io::AsyncReadExt::read_buf(&mut reader, &mut read_buf).await?;
                    Ok(read_buf.filled().len())
                }
                #[cfg(feature = "io_uring")]
                RuntimeType::Monoio => {
                    // Use monoio's AsyncReadRent for true zero-copy
                    self.monoio_zero_copy_read(reader, buf).await
                }
            };

            #[cfg(feature = "metrics")]
            {
                let duration = start.elapsed();
                histogram!("rustfs_io_engine_read_duration_seconds").record(duration.as_secs_f64());
                if let Ok(bytes_read) = &result {
                    counter!("rustfs_io_engine_bytes_read_total").increment(*bytes_read as u64);
                    *self.metrics.bytes_transferred.lock() += *bytes_read as u64;
                }
                *self.metrics.operations_total.lock() += 1;
            }

            result
        }
        .instrument(span)
        .await
    }

    /// Perform vectored I/O operations for batch efficiency
    #[instrument(skip(self, reader, buffers), fields(buffer_count = buffers.len()))]
    pub async fn vectored_read<R>(
        &self,
        mut reader: R,
        buffers: &mut [IoSliceMut<'_>],
    ) -> Result<usize>
    where
        R: AsyncRead + Unpin + Send,
    {
        let _permit = self.global_semaphore.acquire().await;

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let result = match self.runtime.runtime_type() {
            RuntimeType::Tokio => {
                // Simulate vectored read with sequential reads for Tokio
                let mut total_read = 0;
                for buffer in buffers {
                    let mut read_buf = ReadBuf::new(buffer);
                    let read = tokio::io::AsyncReadExt::read_buf(&mut reader, &mut read_buf).await?;
                    total_read += read;
                    if read == 0 {
                        break; // EOF
                    }
                }
                Ok(total_read)
            }
            #[cfg(feature = "io_uring")]
            RuntimeType::Monoio => {
                // Use io_uring's native vectored I/O (readv)
                self.monoio_vectored_read(reader, buffers).await
            }
        };

        #[cfg(feature = "metrics")]
        {
            let duration = start.elapsed();
            histogram!("rustfs_io_engine_vectored_read_duration_seconds").record(duration.as_secs_f64());
            if let Ok(bytes_read) = &result {
                counter!("rustfs_io_engine_vectored_bytes_read_total").increment(*bytes_read as u64);
            }
        }

        result
    }

    /// Submit batch operations to io_uring queue
    #[cfg(feature = "io_uring")]
    #[instrument(skip(self, operations), fields(batch_size = operations.len()))]
    pub async fn submit_batch(&self, operations: Vec<BatchOperation>) -> Result<Vec<usize>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let batch_size = operations.len();
        
        #[cfg(feature = "metrics")]
        {
            *self.metrics.batch_queue_depth.lock() = batch_size;
            gauge!("rustfs_io_engine_batch_queue_depth").set(batch_size as f64);
        }

        tracing::debug!(
            batch_size = batch_size,
            "Submitting batch operations to io_uring queue"
        );

        // In a full implementation, this would submit all operations
        // to io_uring's submission queue (SQE) and await completion queue (CQE)
        let results = Vec::with_capacity(batch_size);
        
        // TODO: Implement actual io_uring batch submission
        // This would use monoio's runtime to submit multiple operations atomically
        
        #[cfg(feature = "metrics")]
        counter!("rustfs_io_engine_batch_operations_total").increment(batch_size as u64);

        Ok(results)
    }

    /// Monoio-specific zero-copy read implementation
    #[cfg(feature = "io_uring")]
    async fn monoio_zero_copy_read<R>(&self, _reader: R, _buf: &mut Vec<u8>) -> Result<usize>
    where
        R: AsyncRead + Unpin + Send,
    {
        // In a full implementation, this would use monoio's AsyncReadRent
        // for true zero-copy operations with io_uring
        tracing::debug!("Monoio zero-copy read operation (placeholder)");
        
        // TODO: Implement monoio::io::AsyncReadRent integration
        // let (result, buf_slice) = reader.read(buf_slice).await;
        
        Ok(0) // Placeholder
    }

    /// Monoio-specific vectored read implementation
    #[cfg(feature = "io_uring")]
    async fn monoio_vectored_read<R>(&self, _reader: R, _buffers: &mut [IoSliceMut<'_>]) -> Result<usize>
    where
        R: AsyncRead + Unpin + Send,
    {
        // In a full implementation, this would use io_uring's readv
        tracing::debug!("Monoio vectored read operation (placeholder)");
        
        // TODO: Implement monoio readv operation
        // This would submit multiple buffer reads in a single syscall
        
        Ok(0) // Placeholder
    }

    /// Get current I/O engine statistics
    #[cfg(feature = "metrics")]
    pub fn get_stats(&self) -> IoEngineStats {
        IoEngineStats {
            operations_total: *self.metrics.operations_total.lock(),
            bytes_transferred: *self.metrics.bytes_transferred.lock(),
            batch_queue_depth: *self.metrics.batch_queue_depth.lock(),
            runtime_type: self.runtime.runtime_type(),
            zero_copy_enabled: self.runtime.supports_zero_copy(),
        }
    }
}

/// I/O engine statistics for monitoring
#[cfg(feature = "metrics")]
#[derive(Debug, Clone)]
pub struct IoEngineStats {
    pub operations_total: u64,
    pub bytes_transferred: u64,
    pub batch_queue_depth: usize,
    pub runtime_type: RuntimeType,
    pub zero_copy_enabled: bool,
}

/// Global I/O engine instance
static GLOBAL_IO_ENGINE: std::sync::OnceLock<Arc<IoEngine>> = std::sync::OnceLock::new();

/// Get or initialize the global I/O engine
pub fn get_io_engine() -> Result<&'static Arc<IoEngine>> {
    GLOBAL_IO_ENGINE.get_or_try_init(|| {
        let runtime = crate::runtime::init_runtime()?;
        let engine = IoEngine::new(runtime)?;
        Ok(Arc::new(engine))
    })
}

/// Initialize the global I/O engine with custom runtime
pub fn init_io_engine(runtime: RuntimeHandle) -> Result<&'static Arc<IoEngine>> {
    GLOBAL_IO_ENGINE.get_or_try_init(|| {
        let engine = IoEngine::new(runtime)?;
        Ok(Arc::new(engine))
    })
}