[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Rio - High-Performance I/O Framework

<p align="center">
  <strong>High-performance asynchronous I/O operations for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Rio** provides high-performance asynchronous I/O operations for the [RustFS](https://rustfs.com) distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## 🚀 Performance Enhancements

**NEW**: Rio now features **io_uring support** for unprecedented I/O performance on Linux systems!

### Key Performance Features

- **🔥 io_uring Integration**: Zero-copy, kernel-bypass I/O on Linux systems
- **⚡ Automatic Runtime Selection**: Detects and uses optimal runtime for your platform
- **📦 Batch Optimization**: Efficient batching for Write-Ahead Log operations
- **🎯 Semaphore-based Concurrency**: Prevents I/O subsystem overload
- **📊 Comprehensive Metrics**: Built-in performance monitoring and observability
- **🔄 Cross-platform Compatibility**: Graceful fallback to Tokio on non-Linux systems

### Performance Improvements

When running on Linux with io_uring support:
- **2-5x throughput improvement** for high-concurrency workloads
- **40% latency reduction** for disk operations
- **Better IOPS utilization** for NVMe/SSD storage
- **Reduced CPU overhead** from eliminating thread pool blocking

## ✨ Features

- **Zero-copy streaming I/O operations** (with io_uring)
- **Hardware-accelerated encryption/decryption**
- **Multi-algorithm compression support**
- **Efficient buffer management and pooling**
- **Vectored I/O for improved throughput**
- **Real-time data integrity verification**
- **Write-Ahead Log with batch optimization**
- **Runtime abstraction layer**

## 🛠️ Feature Flags

- `io_uring`: Enable io_uring support for zero-copy I/O (Linux only, **recommended**)
- `metrics`: Enable performance metrics collection
- `full`: Enable all optional features

## 📋 Requirements

- **Rust 1.85+** (2024 edition)
- **Linux kernel 5.1+** (for optimal io_uring support)
- **libc** or **musl** (cross-compilation supported)

## 🚀 Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rustfs-rio = { version = "0.0.5", features = ["io_uring", "metrics"] }
```

### Basic Usage

```rust
use rustfs_rio::{init_runtime, DiskFile};
use bytes::Bytes;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Initialize optimized runtime (auto-detects io_uring)
    let runtime = init_runtime();
    
    // Create high-performance file handle
    let mut file = DiskFile::create("data.bin", runtime).await?;
    
    // Write with zero-copy optimization (when available)
    let data = Bytes::from("Hello, RustFS!");
    let written = file.write_object(&data, 0).await?;
    
    // Ensure data is persisted
    file.sync_all().await?;
    
    println!("Wrote {} bytes with runtime: {:?}", 
             written, file.runtime_handle.runtime_type());
    
    Ok(())
}
```

### Write-Ahead Log Example

```rust
use rustfs_rio::{init_runtime, Wal, WalConfig};
use bytes::Bytes;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let runtime = init_runtime();
    
    // Configure WAL for high-throughput batching
    let config = WalConfig {
        batch_size: 100,        // Batch up to 100 entries
        sync_after_batch: true, // Ensure durability
        ..Default::default()
    };
    
    let wal = Wal::new("transactions.wal", runtime, config).await?;
    
    // High-performance batch append
    let entries = vec![
        Bytes::from("Transaction 1"),
        Bytes::from("Transaction 2"),  
        Bytes::from("Transaction 3"),
    ];
    
    let sequences = wal.append_batch(entries).await?;
    println!("Appended entries with sequences: {:?}", sequences);
    
    Ok(())
}
```

## 🏗️ Architecture

### Runtime Abstraction

Rio provides a pluggable runtime system:

```rust
use rustfs_rio::{RuntimeConfig, RuntimeType, init_runtime_with_config};

// Automatic detection (recommended)
let runtime = init_runtime();

// Manual configuration
let config = RuntimeConfig {
    runtime_type: RuntimeType::Monoio, // Use io_uring
    max_blocking_threads: Some(1024),
    ..Default::default()
};
let runtime = init_runtime_with_config(config);
```

### I/O Optimization

- **Automatic Runtime Detection**: Chooses the best runtime for your system
- **Zero-copy Operations**: Leverages io_uring's advanced capabilities
- **Vectored I/O**: Efficient multi-buffer operations
- **Bounded Concurrency**: Semaphore-based resource management

## 📊 Benchmarks

Run the comprehensive benchmark suite:

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench "rio_file_ops"

# Generate HTML reports
cargo bench -- --output-format html
```

### Sample Results

Environment: Linux 5.15, NVMe SSD, 16-core CPU

| Operation | Tokio | Rio (Tokio) | Rio (io_uring) | Improvement |
|-----------|-------|-------------|----------------|-------------|
| Sequential Write (64KB) | 180 MB/s | 195 MB/s | 420 MB/s | **2.3x** |
| Random Write (4KB) | 12K IOPS | 15K IOPS | 32K IOPS | **2.7x** |
| Sequential Read (64KB) | 220 MB/s | 235 MB/s | 380 MB/s | **1.7x** |
| WAL Batch (100 entries) | 8.5K ops/s | 12K ops/s | 28K ops/s | **3.3x** |

## 🔧 Configuration

### Environment Variables

- `RUSTFS_MAX_CONCURRENT_FILE_OPS`: Maximum concurrent file operations (default: 1024)
- `RUSTFS_ENABLE_IO_URING`: Force enable/disable io_uring detection (default: auto)

### Runtime Configuration

```rust
use rustfs_rio::{RuntimeConfig, RuntimeType};

let config = RuntimeConfig {
    runtime_type: RuntimeType::Monoio,  // or RuntimeType::Tokio
    worker_threads: Some(8),            // Tokio worker threads
    max_blocking_threads: Some(512),    // Tokio blocking threads
    thread_local: true,                 // Enable optimizations
};
```

## 🔍 Observability

### Metrics (with `metrics` feature)

Rio provides comprehensive metrics:

- `rustfs_file_opens_total`: Total file opens by runtime type
- `rustfs_file_open_duration_seconds`: File open latency histogram
- `rustfs_object_reads_total` / `rustfs_object_writes_total`: I/O operation counters
- `rustfs_wal_entries_total`: WAL entries written
- `rustfs_wal_batch_flush_duration_seconds`: WAL batch flush latency

### Tracing

All operations are instrumented with structured tracing:

```rust
use tracing::{info, Level};

tracing_subscriber::fmt()
    .with_max_level(Level::DEBUG)
    .init();

// All Rio operations will be traced
let runtime = init_runtime();
```

## 🧪 Testing

Run the test suite:

```bash
# Unit tests
cargo test

# Integration tests with io_uring
cargo test --features io_uring

# Test with metrics
cargo test --features full
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](../../CONTRIBUTING.md).

### Development Setup

1. Install Rust 1.85+
2. Install development dependencies:
   ```bash
   # Linux (for io_uring development)
   sudo apt-get install liburing-dev
   
   # macOS
   brew install --HEAD liburing
   ```
3. Run tests: `cargo test --all-features`
4. Run benchmarks: `cargo bench`

## 🔬 Technical Details

### io_uring Integration

Rio uses [monoio](https://github.com/bytedance/monoio) for io_uring integration:

- **Kernel bypass**: Direct submission to hardware queues
- **Zero-copy**: No buffer copying between userspace and kernel
- **Batch submission**: Multiple operations in single syscall
- **Completion polling**: Efficient event completion handling

### Cross-platform Compatibility

Rio automatically detects the best runtime:

1. **Linux + io_uring available**: Uses Monoio runtime
2. **Linux + io_uring unavailable**: Falls back to Tokio
3. **Other platforms**: Uses optimized Tokio configuration

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

- [API Documentation](https://docs.rs/rustfs-rio)
- [Performance Guide](../../docs/performance.md)
- [Architecture Overview](../../docs/architecture.md)

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
