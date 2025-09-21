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

//! High-performance disk I/O operations with io_uring support
//!
//! This module provides optimized file operations that can leverage io_uring
//! on Linux systems for zero-copy, high-throughput disk I/O while maintaining
//! compatibility with tokio on other platforms.

use crate::runtime::{RuntimeHandle, RuntimeType};
use std::io::Result as IoResult;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tracing::{debug, instrument};

#[cfg(feature = "metrics")]
use metrics::{counter, histogram};

/// Global semaphore for controlling concurrent file operations
/// This prevents overwhelming the I/O subsystem with too many concurrent operations
static GLOBAL_FILE_SEMAPHORE: std::sync::OnceLock<async_semaphore::Semaphore> = std::sync::OnceLock::new();

/// Get or initialize the global file operation semaphore
fn get_file_semaphore() -> &'static async_semaphore::Semaphore {
    GLOBAL_FILE_SEMAPHORE.get_or_init(|| {
        let permits = std::env::var("RUSTFS_MAX_CONCURRENT_FILE_OPS")
            .and_then(|s| s.parse().map_err(|_| std::env::VarError::NotPresent))
            .unwrap_or(1024); // Default to 1024 concurrent file operations
        
        tracing::info!("Initializing file operation semaphore with {} permits", permits);
        async_semaphore::Semaphore::new(permits)
    })
}

/// Trait for high-performance async file operations
///
/// This trait abstracts over different runtime implementations to provide
/// optimal performance on each platform.
#[async_trait::async_trait]
pub trait AsyncFile: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin {
    /// Read data from the file at a specific offset
    async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> IoResult<usize>;
    
    /// Write data to the file at a specific offset
    async fn write_at(&mut self, buf: &[u8], offset: u64) -> IoResult<usize>;
    
    /// Read data into a vector with zero-copy optimization when available
    async fn read_vectored_at(&mut self, bufs: &mut [std::io::IoSliceMut<'_>], offset: u64) -> IoResult<usize>;
    
    /// Write data from multiple buffers with zero-copy optimization when available
    async fn write_vectored_at(&mut self, bufs: &[std::io::IoSlice<'_>], offset: u64) -> IoResult<usize>;
    
    /// Sync all data to disk
    async fn sync_all(&mut self) -> IoResult<()>;
    
    /// Sync data (but not necessarily metadata) to disk
    async fn sync_data(&mut self) -> IoResult<()>;
    
    /// Get file metadata
    async fn metadata(&self) -> IoResult<std::fs::Metadata>;
    
    /// Set the file length, truncating or extending as needed
    async fn set_len(&mut self, size: u64) -> IoResult<()>;
}

/// High-performance file implementation that adapts to the available runtime
pub struct DiskFile {
    inner: DiskFileInner,
    runtime_handle: RuntimeHandle,
}

enum DiskFileInner {
    Tokio(tokio::fs::File),
    #[cfg(feature = "io_uring")]
    Monoio(monoio::fs::File),
}

impl DiskFile {
    /// Open a file with optimized settings for the current runtime
    #[instrument(skip(runtime_handle), fields(path = %path.as_ref().display()))]
    pub async fn open<P: AsRef<Path>>(path: P, runtime_handle: RuntimeHandle) -> IoResult<Self> {
        let _permit = get_file_semaphore().acquire().await;
        
        #[cfg(feature = "metrics")]
        counter!("rustfs_file_opens_total", "runtime" => format!("{:?}", runtime_handle.runtime_type())).increment(1);
        
        let _start = std::time::Instant::now();
        
        let inner = match runtime_handle.runtime_type() {
            RuntimeType::Tokio => {
                let file = tokio::fs::File::open(path.as_ref()).await?;
                DiskFileInner::Tokio(file)
            }
            #[cfg(feature = "io_uring")]
            RuntimeType::Monoio => {
                // In a real implementation, we'd use monoio::fs::File::open
                // For now, fall back to tokio
                tracing::warn!("Monoio not fully implemented, falling back to Tokio");
                let file = tokio::fs::File::open(path.as_ref()).await?;
                DiskFileInner::Tokio(file)
            }
        };
        
        #[cfg(feature = "metrics")]
        histogram!("rustfs_file_open_duration_seconds").record(_start.elapsed().as_secs_f64());
        
        debug!("Opened file: {}", path.as_ref().display());
        
        Ok(Self {
            inner,
            runtime_handle,
        })
    }
    
    /// Create a new file with optimized settings
    #[instrument(skip(runtime_handle), fields(path = %path.as_ref().display()))]
    pub async fn create<P: AsRef<Path>>(path: P, runtime_handle: RuntimeHandle) -> IoResult<Self> {
        let _permit = get_file_semaphore().acquire().await;
        
        #[cfg(feature = "metrics")]
        counter!("rustfs_file_creates_total", "runtime" => format!("{:?}", runtime_handle.runtime_type())).increment(1);
        
        let _start = std::time::Instant::now();
        
        let inner = match runtime_handle.runtime_type() {
            RuntimeType::Tokio => {
                let file = tokio::fs::File::create(path.as_ref()).await?;
                DiskFileInner::Tokio(file)
            }
            #[cfg(feature = "io_uring")]
            RuntimeType::Monoio => {
                // In a real implementation, we'd use monoio::fs::File::create
                tracing::warn!("Monoio not fully implemented, falling back to Tokio");
                let file = tokio::fs::File::create(path.as_ref()).await?;
                DiskFileInner::Tokio(file)
            }
        };
        
        #[cfg(feature = "metrics")]
        histogram!("rustfs_file_create_duration_seconds").record(_start.elapsed().as_secs_f64());
        
        debug!("Created file: {}", path.as_ref().display());
        
        Ok(Self {
            inner,
            runtime_handle,
        })
    }
    
    /// Read object data with optimized I/O patterns
    #[instrument(skip(self, buf))]
    pub async fn read_object(&mut self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
        let _guard = scopeguard::guard((), |_| {
            #[cfg(feature = "metrics")]
            counter!("rustfs_object_reads_total").increment(1);
        });
        
        let _start = std::time::Instant::now();
        let result = self.read_at(buf, offset).await;
        
        #[cfg(feature = "metrics")]
        histogram!("rustfs_object_read_duration_seconds").record(_start.elapsed().as_secs_f64());
        
        result
    }
    
    /// Write object data with optimized I/O patterns
    #[instrument(skip(self, data))]
    pub async fn write_object(&mut self, data: &[u8], offset: u64) -> IoResult<usize> {
        let _guard = scopeguard::guard((), |_| {
            #[cfg(feature = "metrics")]
            counter!("rustfs_object_writes_total").increment(1);
        });
        
        let _start = std::time::Instant::now();
        let result = self.write_at(data, offset).await;
        
        #[cfg(feature = "metrics")]
        histogram!("rustfs_object_write_duration_seconds").record(_start.elapsed().as_secs_f64());
        
        result
    }
    
    /// Bounded write operation to prevent resource exhaustion
    #[instrument(skip(self, data))]
    pub async fn bounded_write(&mut self, data: &[u8], offset: u64) -> IoResult<usize> {
        let _permit = get_file_semaphore().acquire().await;
        self.write_object(data, offset).await
    }
}

#[async_trait::async_trait]
impl AsyncFile for DiskFile {
    async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => {
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.read(buf).await
            }
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(_file) => {
                // In a real implementation, we'd use monoio's read_at
                // which provides true zero-copy I/O with io_uring
                todo!("Monoio read_at implementation")
            }
        }
    }
    
    async fn write_at(&mut self, buf: &[u8], offset: u64) -> IoResult<usize> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => {
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.write(buf).await
            }
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(_file) => {
                // In a real implementation, we'd use monoio's write_at
                todo!("Monoio write_at implementation")
            }
        }
    }
    
    async fn read_vectored_at(&mut self, bufs: &mut [std::io::IoSliceMut<'_>], offset: u64) -> IoResult<usize> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => {
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                // Tokio doesn't provide read_vectored directly, so we'll read into each buffer sequentially
                let mut total_read = 0;
                for buf in bufs {
                    match file.read(buf).await {
                        Ok(0) => break, // EOF
                        Ok(n) => total_read += n,
                        Err(e) => return Err(e),
                    }
                }
                Ok(total_read)
            }
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(_file) => {
                // io_uring provides excellent vectored I/O support
                todo!("Monoio vectored read implementation")
            }
        }
    }
    
    async fn write_vectored_at(&mut self, bufs: &[std::io::IoSlice<'_>], offset: u64) -> IoResult<usize> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => {
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                // Tokio doesn't provide write_vectored directly, so we'll write each buffer sequentially  
                let mut total_written = 0;
                for buf in bufs {
                    let written = file.write(buf).await?;
                    total_written += written;
                    if written != buf.len() {
                        break; // Partial write
                    }
                }
                Ok(total_written)
            }
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(_file) => {
                // io_uring provides excellent vectored I/O support
                todo!("Monoio vectored write implementation")
            }
        }
    }
    
    async fn sync_all(&mut self) -> IoResult<()> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => file.sync_all().await,
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => file.sync_all().await,
        }
    }
    
    async fn sync_data(&mut self) -> IoResult<()> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => file.sync_data().await,
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => file.sync_data().await,
        }
    }
    
    async fn metadata(&self) -> IoResult<std::fs::Metadata> {
        match &self.inner {
            DiskFileInner::Tokio(file) => file.metadata().await,
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => file.metadata().await,
        }
    }
    
    async fn set_len(&mut self, size: u64) -> IoResult<()> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => file.set_len(size).await,
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => file.set_len(size).await,
        }
    }
}

// Implement standard async traits
impl AsyncRead for DiskFile {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => Pin::new(file).poll_read(cx, buf),
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => Pin::new(file).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for DiskFile {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => Pin::new(file).poll_write(cx, buf),
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => Pin::new(file).poll_write(cx, buf),
        }
    }
    
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => Pin::new(file).poll_flush(cx),
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => Pin::new(file).poll_flush(cx),
        }
    }
    
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => Pin::new(file).poll_shutdown(cx),
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => Pin::new(file).poll_shutdown(cx),
        }
    }
}

impl AsyncSeek for DiskFile {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> IoResult<()> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => Pin::new(file).start_seek(position),
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => Pin::new(file).start_seek(position),
        }
    }
    
    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        match &mut self.inner {
            DiskFileInner::Tokio(file) => Pin::new(file).poll_complete(cx),
            #[cfg(feature = "io_uring")]
            DiskFileInner::Monoio(file) => Pin::new(file).poll_complete(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::init_runtime;
    use tempfile::NamedTempFile;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    
    #[tokio::test]
    async fn test_disk_file_operations() {
        let runtime = init_runtime();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        // Test file creation and writing
        let mut file = DiskFile::create(path, runtime.clone()).await.unwrap();
        let data = b"Hello, RustFS!";
        let written = file.write_object(data, 0).await.unwrap();
        assert_eq!(written, data.len());
        
        file.sync_all().await.unwrap();
        drop(file);
        
        // Test file opening and reading
        let mut file = DiskFile::open(path, runtime).await.unwrap();
        let mut buf = vec![0u8; data.len()];
        let read = file.read_object(&mut buf, 0).await.unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);
    }
    
    #[tokio::test]
    async fn test_bounded_write() {
        let runtime = init_runtime();
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut file = DiskFile::create(path, runtime).await.unwrap();
        let data = b"Bounded write test";
        let written = file.bounded_write(data, 0).await.unwrap();
        assert_eq!(written, data.len());
    }
}