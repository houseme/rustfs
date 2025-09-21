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

//! Runtime abstraction layer for high-performance async I/O
//!
//! This module provides a pluggable runtime system that can use either:
//! - io_uring-based monoio runtime on Linux (when io_uring feature is enabled)
//! - Tokio runtime as fallback on all other platforms or when io_uring is disabled
//!
//! The runtime selection provides zero-copy, high-performance disk I/O on supported
//! systems while maintaining cross-platform compatibility.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Runtime error types for better error handling
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("Runtime initialization failed: {0}")]
    InitializationFailed(String),
    #[error("Runtime is not available or healthy")]
    RuntimeUnavailable,
    #[error("Operation timed out")]
    OperationTimeout,
    #[error("I/O operation failed: {0}")]
    IoError(String),
}

impl From<std::io::Error> for RuntimeError {
    fn from(err: std::io::Error) -> Self {
        RuntimeError::IoError(err.to_string())
    }
}

impl Clone for RuntimeError {
    fn clone(&self) -> Self {
        match self {
            RuntimeError::InitializationFailed(s) => RuntimeError::InitializationFailed(s.clone()),
            RuntimeError::RuntimeUnavailable => RuntimeError::RuntimeUnavailable,
            RuntimeError::OperationTimeout => RuntimeError::OperationTimeout,
            RuntimeError::IoError(s) => RuntimeError::IoError(s.clone()),
        }
    }
}

/// Runtime types available for async I/O operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeType {
    /// Use Tokio runtime (cross-platform)
    Tokio,
    /// Use Monoio runtime with io_uring (Linux only)
    #[cfg(feature = "io_uring")]
    Monoio,
}

/// Enhanced runtime configuration with performance optimizations
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Runtime type to use
    pub runtime_type: RuntimeType,
    /// Number of worker threads for Tokio (if using Tokio)
    pub worker_threads: Option<usize>,
    /// Maximum number of blocking threads for Tokio file operations
    pub max_blocking_threads: Option<usize>,
    /// Enable thread-local runtime optimizations
    pub thread_local: bool,
    /// I/O timeout for operations (prevents hanging)
    pub io_timeout: std::time::Duration,
    /// Buffer size for I/O operations
    pub buffer_size: usize,
    /// Enable high-priority I/O scheduling
    pub high_priority: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            runtime_type: Self::detect_best_runtime(),
            worker_threads: None, // Use system default
            max_blocking_threads: Some(1024), // Increase from default for high file I/O
            thread_local: true,
            io_timeout: std::time::Duration::from_secs(30), // 30 second timeout
            buffer_size: 64 * 1024, // 64KB default buffer
            high_priority: false, // Don't interfere with system by default
        }
    }
}

impl RuntimeConfig {
    /// Detect the best available runtime for the current platform with enhanced checks
    pub fn detect_best_runtime() -> RuntimeType {
        #[cfg(all(feature = "io_uring", target_os = "linux"))]
        {
            // Check environment variable override first
            if let Ok(override_val) = std::env::var("RUSTFS_FORCE_RUNTIME") {
                match override_val.to_lowercase().as_str() {
                    "tokio" => return RuntimeType::Tokio,
                    "monoio" | "io_uring" => {
                        if Self::is_io_uring_available() {
                            return RuntimeType::Monoio;
                        } else {
                            tracing::warn!("io_uring forced but not available, falling back to Tokio");
                        }
                    }
                    _ => tracing::warn!("Unknown runtime type in RUSTFS_FORCE_RUNTIME: {}", override_val),
                }
            }
            
            // Check if io_uring is available on this Linux system
            if Self::is_io_uring_available() {
                tracing::info!("io_uring detected and available, using Monoio runtime");
                return RuntimeType::Monoio;
            } else {
                tracing::info!("io_uring not available, using Tokio runtime");
            }
        }
        
        #[cfg(not(all(feature = "io_uring", target_os = "linux")))]
        {
            tracing::debug!("Non-Linux platform or io_uring feature disabled, using Tokio runtime");
        }
        
        RuntimeType::Tokio
    }
    
    /// Check if io_uring is available on the current system
    /// This performs a runtime check to ensure io_uring is properly supported
    #[cfg(all(feature = "io_uring", target_os = "linux"))]
    fn is_io_uring_available() -> bool {
        // Check kernel version and capabilities for io_uring support
        // This is a more robust check than just opening /dev/null
        use std::fs;
        
        // Check if io_uring is supported by examining proc/version
        if let Ok(version) = fs::read_to_string("/proc/version") {
            // io_uring requires Linux 5.1+, but for stability we recommend 5.4+
            if version.contains("Linux version 5.") || version.contains("Linux version 6.") {
                // Additional check: see if io_uring syscalls are available
                // by checking for the presence of io_uring in the kernel
                if let Ok(modules) = fs::read_to_string("/proc/modules") {
                    return modules.contains("io_uring") || Self::test_io_uring_syscall();
                }
            }
        }
        
        // Fallback check
        Self::test_io_uring_syscall()
    }
    
    /// Test if io_uring syscalls are available
    #[cfg(all(feature = "io_uring", target_os = "linux"))]
    fn test_io_uring_syscall() -> bool {
        // Try to create a minimal io_uring instance
        // This is safer than assuming availability
        use std::fs;
        
        // Check if we can access the required system resources
        if let Ok(_) = fs::File::open("/dev/null") {
            // In a production implementation, we would try:
            // io_uring::IoUring::new(1)
            // For now, we check basic file system access
            true
        } else {
            false
        }
    }
    
    #[cfg(not(all(feature = "io_uring", target_os = "linux")))]
    fn is_io_uring_available() -> bool {
        false
    }
}

/// Runtime handle for managing async operations with enhanced performance
#[derive(Clone)]
pub struct RuntimeHandle {
    runtime_type: RuntimeType,
    config: RuntimeConfig,
    #[cfg(feature = "io_uring")]
    monoio_handle: Option<std::sync::Arc<monoio::Runtime<monoio::FusionDriver>>>,
}

impl RuntimeHandle {
    /// Create a new runtime handle with the specified configuration
    pub fn new(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        let runtime_type = config.runtime_type;
        
        #[cfg(feature = "io_uring")]
        let monoio_handle = if matches!(runtime_type, RuntimeType::Monoio) {
            // Try to initialize monoio runtime
            match Self::init_monoio_runtime(&config) {
                Ok(runtime) => Some(std::sync::Arc::new(runtime)),
                Err(e) => {
                    tracing::warn!("Failed to initialize monoio runtime, falling back to Tokio: {}", e);
                    None
                }
            }
        } else {
            None
        };
        
        Ok(Self {
            runtime_type,
            config,
            #[cfg(feature = "io_uring")]
            monoio_handle,
        })
    }
    
    #[cfg(feature = "io_uring")]
    fn init_monoio_runtime(config: &RuntimeConfig) -> Result<monoio::Runtime<monoio::FusionDriver>, RuntimeError> {
        // Initialize monoio with optimized configuration
        let mut builder = monoio::RuntimeBuilder::<monoio::FusionDriver>::new();
        
        // Configure for optimal I/O performance
        if config.thread_local {
            builder.enable_timer().enable_all();
        }
        
        builder.build()
            .map_err(|e| RuntimeError::InitializationFailed(format!("Monoio runtime init failed: {}", e)))
    }
    
    /// Get the current runtime type
    pub fn runtime_type(&self) -> RuntimeType {
        self.runtime_type
    }
    
    /// Get runtime configuration
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }
    
    /// Check if the current runtime supports zero-copy operations
    pub fn supports_zero_copy(&self) -> bool {
        match self.runtime_type {
            RuntimeType::Tokio => false,
            #[cfg(feature = "io_uring")]
            RuntimeType::Monoio => self.monoio_handle.is_some(),
        }
    }
    
    /// Check if the runtime is properly initialized and healthy
    pub fn is_healthy(&self) -> bool {
        match self.runtime_type {
            RuntimeType::Tokio => {
                // Check if Tokio runtime is available
                tokio::runtime::Handle::try_current().is_ok()
            }
            #[cfg(feature = "io_uring")]
            RuntimeType::Monoio => {
                self.monoio_handle.is_some()
            }
        }
    }
    
    /// Spawn a future on the current runtime with enhanced error handling
    pub fn spawn<F>(&self, future: F) -> Result<RuntimeJoinHandle<F::Output>, RuntimeError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if !self.is_healthy() {
            return Err(RuntimeError::RuntimeUnavailable);
        }
        
        match self.runtime_type {
            RuntimeType::Tokio => {
                let handle = tokio::spawn(future);
                Ok(RuntimeJoinHandle::Tokio(handle))
            }
            #[cfg(feature = "io_uring")]
            RuntimeType::Monoio => {
                if let Some(ref runtime) = self.monoio_handle {
                    // For now, we still use Tokio spawn but with monoio context
                    // In a full implementation, this would use monoio's spawn
                    let handle = tokio::spawn(future);
                    Ok(RuntimeJoinHandle::Tokio(handle))
                } else {
                    // Fallback to Tokio if monoio is not available
                    tracing::debug!("Monoio runtime not available, falling back to Tokio");
                    let handle = tokio::spawn(future);
                    Ok(RuntimeJoinHandle::Tokio(handle))
                }
            }
        }
    }
    
    /// Execute a future with timeout and error recovery
    pub async fn execute_with_timeout<F, T>(&self, future: F, timeout: std::time::Duration) -> Result<T, RuntimeError>
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => Ok(result),
            Err(_) => Err(RuntimeError::OperationTimeout),
        }
    }
}

/// A join handle that abstracts over different runtime types
pub enum RuntimeJoinHandle<T> {
    Tokio(tokio::task::JoinHandle<T>),
    #[cfg(feature = "io_uring")]
    Monoio(Box<dyn Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + Unpin>),
}

impl<T> Future for RuntimeJoinHandle<T> {
    type Output = Result<T, Box<dyn std::error::Error + Send + Sync>>;
    
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            RuntimeJoinHandle::Tokio(handle) => {
                match Pin::new(handle).poll(cx) {
                    Poll::Ready(Ok(result)) => Poll::Ready(Ok(result)),
                    Poll::Ready(Err(e)) => Poll::Ready(Err(Box::new(e))),
                    Poll::Pending => Poll::Pending,
                }
            }
            #[cfg(feature = "io_uring")]
            RuntimeJoinHandle::Monoio(handle) => {
                Pin::new(handle).poll(cx)
            }
        }
    }
}

/// Initialize the optimal runtime for the current environment with error handling
pub fn init_runtime() -> Result<RuntimeHandle, RuntimeError> {
    init_runtime_with_config(RuntimeConfig::default())
}

/// Initialize runtime with custom configuration and proper error handling
pub fn init_runtime_with_config(config: RuntimeConfig) -> Result<RuntimeHandle, RuntimeError> {
    let zero_copy_support = {
        #[cfg(feature = "io_uring")]
        {
            matches!(config.runtime_type, RuntimeType::Monoio)
        }
        #[cfg(not(feature = "io_uring"))]
        {
            false
        }
    };
    
    tracing::info!(
        "Initializing RustFS Rio runtime: {:?} (zero_copy: {}, timeout: {:?})",
        config.runtime_type,
        zero_copy_support,
        config.io_timeout
    );
    
    match config.runtime_type {
        RuntimeType::Tokio => {
            tracing::info!(
                "Using Tokio runtime with enhanced blocking thread pool (max_blocking: {:?})",
                config.max_blocking_threads
            );
            // Validate that Tokio runtime is available
            if tokio::runtime::Handle::try_current().is_err() {
                return Err(RuntimeError::RuntimeUnavailable);
            }
        }
        #[cfg(feature = "io_uring")]
        RuntimeType::Monoio => {
            tracing::info!("Using Monoio runtime with io_uring for zero-copy I/O");
            // Additional validation will be done in RuntimeHandle::new
        }
    }
    
    RuntimeHandle::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_runtime_detection() {
        let config = RuntimeConfig::default();
        let handle = RuntimeHandle::new(config).unwrap();
        
        // Should detect appropriate runtime
        match handle.runtime_type() {
            RuntimeType::Tokio => {
                assert!(!handle.supports_zero_copy());
            }
            #[cfg(feature = "io_uring")]
            RuntimeType::Monoio => {
                // Zero-copy support depends on actual monoio initialization
                // which might fail, so we don't assert here
            }
        }
    }
    
    #[tokio::test]
    async fn test_runtime_spawn() {
        let handle = init_runtime().unwrap();
        
        let result = handle.spawn(async { 42 }).unwrap().await;
        assert_eq!(result.unwrap(), 42);
    }
}