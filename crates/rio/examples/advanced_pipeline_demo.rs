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

//! Advanced RustFS Rio Pipeline Demo
//!
//! This example demonstrates the sophisticated I/O pipeline with io_uring optimization,
//! zero-copy operations, and comprehensive monitoring for high-performance object storage.

use anyhow::Result;
use bytes::Bytes;
use http::{HeaderMap, Method};
use rustfs_rio::{
    CompressReader, DiskFile, EncryptReader, EtagReader, HttpReader, RuntimeConfig, RuntimeType, Wal, WalConfig, WarpReader,
    get_io_engine, init_runtime,
};
use rustfs_utils::compress::CompressionAlgorithm;
use std::io::Cursor;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, warn};

#[cfg(feature = "metrics")]
use rustfs_rio::{HttpDownloadStats, IoEngineStats};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize enhanced tracing
    tracing_subscriber::fmt()
        .with_env_filter("rustfs_rio=debug,advanced_pipeline_demo=info")
        .init();

    info!("🚀 RustFS Rio Advanced Pipeline Demo Starting");

    // Initialize optimized runtime with io_uring detection
    let runtime = init_runtime()?;
    info!(
        "Runtime initialized: {:?} (zero_copy: {})",
        runtime.runtime_type(),
        runtime.supports_zero_copy()
    );

    // Initialize global I/O engine
    let io_engine = get_io_engine()?;
    info!("I/O engine initialized with advanced optimizations");

    // Demonstrate various pipeline configurations
    demo_etag_processing().await?;
    demo_compression_pipeline().await?;
    demo_encryption_pipeline().await?;
    demo_combined_pipeline().await?;
    demo_concurrent_operations().await?;
    demo_wal_operations().await?;

    #[cfg(feature = "metrics")]
    print_performance_statistics(&io_engine).await?;

    info!("✅ Advanced pipeline demo completed successfully");
    Ok(())
}

/// Demonstrate enhanced ETag processing with batch MD5 optimization
async fn demo_etag_processing() -> Result<()> {
    info!("📝 Demonstrating Enhanced ETag Processing");

    let data = generate_large_object(10 * 1024 * 1024); // 10MB object
    let reader = Cursor::new(&data);
    let warp_reader = Box::new(WarpReader::new(reader));

    let start = Instant::now();

    // Create ETag reader with optimized buffer size
    let mut etag_reader = EtagReader::with_buffer_size(
        warp_reader,
        None,       // Let it compute the ETag
        128 * 1024, // 128KB buffer for zero-copy operations
    );

    let mut result = Vec::new();
    etag_reader.read_to_end(&mut result).await?;
    let etag = etag_reader.get_etag();

    let duration = start.elapsed();

    info!("ETag processing: {} bytes -> ETag: {} (took {:?})", result.len(), etag, duration);
    info!(
        "Throughput: {:.2} MB/s",
        (result.len() as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
    );

    Ok(())
}

/// Demonstrate compression pipeline with different algorithms
async fn demo_compression_pipeline() -> Result<()> {
    info!("🗜️ Demonstrating Advanced Compression Pipeline");

    let data = generate_compressible_object(20 * 1024 * 1024); // 20MB compressible data

    for algorithm in [CompressionAlgorithm::Deflate, CompressionAlgorithm::Gzip] {
        let reader = Cursor::new(&data);
        let warp_reader = WarpReader::new(reader);

        let start = Instant::now();

        // Create compression reader with optimized block size
        let mut compress_reader = CompressReader::with_block_size(
            warp_reader,
            1024 * 1024, // 1MB blocks for optimal throughput
            algorithm,
        );

        let mut compressed_data = Vec::new();
        compress_reader.read_to_end(&mut compressed_data).await?;

        let duration = start.elapsed();
        let compression_ratio = compressed_data.len() as f64 / data.len() as f64;

        info!(
            "Compression {:?}: {} -> {} bytes (ratio: {:.2}, took {:?})",
            algorithm,
            data.len(),
            compressed_data.len(),
            compression_ratio,
            duration
        );
        info!(
            "Compression throughput: {:.2} MB/s",
            (data.len() as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
        );
    }

    Ok(())
}

/// Demonstrate encryption pipeline with AES-256-GCM
async fn demo_encryption_pipeline() -> Result<()> {
    info!("🔐 Demonstrating Advanced Encryption Pipeline");

    let data = generate_large_object(15 * 1024 * 1024); // 15MB object
    let reader = Cursor::new(&data);
    let warp_reader = WarpReader::new(reader);

    // Generate secure key and nonce (in production, use proper key management)
    let key = [42u8; 32]; // AES-256 key
    let nonce = [24u8; 12]; // 96-bit GCM nonce

    let start = Instant::now();

    // Create encryption reader with optimized block size
    let mut encrypt_reader = EncryptReader::with_block_size(
        warp_reader,
        key,
        nonce,
        256 * 1024, // 256KB encryption blocks
    );

    let mut encrypted_data = Vec::new();
    encrypt_reader.read_to_end(&mut encrypted_data).await?;

    let duration = start.elapsed();
    let overhead = encrypted_data.len() as f64 / data.len() as f64 - 1.0;

    info!(
        "Encryption: {} -> {} bytes (overhead: {:.1}%, took {:?})",
        data.len(),
        encrypted_data.len(),
        overhead * 100.0,
        duration
    );
    info!(
        "Encryption throughput: {:.2} MB/s",
        (data.len() as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
    );

    Ok(())
}

/// Demonstrate combined processing pipeline
async fn demo_combined_pipeline() -> Result<()> {
    info!("🔄 Demonstrating Combined Processing Pipeline");

    let data = generate_compressible_object(50 * 1024 * 1024); // 50MB object
    let reader = Cursor::new(&data);
    let warp_reader = WarpReader::new(reader);

    let start = Instant::now();

    // Build sophisticated processing pipeline
    // Step 1: Compression
    let compress_reader = CompressReader::with_block_size(
        warp_reader,
        1024 * 1024, // 1MB compression blocks
        CompressionAlgorithm::Deflate,
    );

    // Step 2: Encryption
    let key = [1u8; 32];
    let nonce = [2u8; 12];
    let encrypt_reader = EncryptReader::with_block_size(
        compress_reader,
        key,
        nonce,
        512 * 1024, // 512KB encryption blocks
    );

    // Step 3: ETag computation
    let mut etag_reader = EtagReader::with_buffer_size(
        Box::new(encrypt_reader),
        None,
        256 * 1024, // 256KB ETag buffer
    );

    let mut final_data = Vec::new();
    etag_reader.read_to_end(&mut final_data).await?;
    let etag = etag_reader.get_etag();

    let duration = start.elapsed();

    info!(
        "Combined pipeline: {} -> {} bytes, ETag: {} (took {:?})",
        data.len(),
        final_data.len(),
        etag,
        duration
    );
    info!(
        "Pipeline throughput: {:.2} MB/s",
        (data.len() as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
    );

    Ok(())
}

/// Demonstrate concurrent operations scaling
async fn demo_concurrent_operations() -> Result<()> {
    info!("⚡ Demonstrating Concurrent Operations Scaling");

    let object_size = 5 * 1024 * 1024; // 5MB per object
    let concurrent_operations = 16;

    let start = Instant::now();

    // Spawn multiple concurrent processing tasks
    let tasks = (0..concurrent_operations)
        .map(|i| {
            let data = generate_large_object(object_size);

            tokio::spawn(async move {
                let reader = Cursor::new(&data);
                let warp_reader = Box::new(WarpReader::new(reader));

                // Each task processes with ETag computation
                let mut etag_reader = EtagReader::with_buffer_size(warp_reader, None, 64 * 1024);

                let mut result = Vec::new();
                etag_reader.read_to_end(&mut result).await.unwrap();
                let etag = etag_reader.get_etag();

                (i, result.len(), etag)
            })
        })
        .collect::<Vec<_>>();

    let results = futures::future::join_all(tasks).await;
    let duration = start.elapsed();

    let total_bytes: usize = results.iter().map(|r| r.as_ref().unwrap().1).sum();

    info!(
        "Concurrent operations: {} tasks, {} total bytes (took {:?})",
        concurrent_operations, total_bytes, duration
    );
    info!(
        "Concurrent throughput: {:.2} MB/s",
        (total_bytes as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
    );

    Ok(())
}

/// Demonstrate Write-Ahead Log operations with batch optimization
async fn demo_wal_operations() -> Result<()> {
    info!("📋 Demonstrating WAL Operations with Batch Optimization");

    let runtime = init_runtime()?;
    let temp_file = NamedTempFile::new()?;

    // Configure WAL for high-performance batch operations
    let wal_config = WalConfig {
        batch_size: 200,                         // Large batches for efficiency
        flush_timeout: Duration::from_millis(5), // Fast flush for low latency
        sync_after_batch: true,                  // Ensure durability
        buffer_size: 1024 * 1024,                // 1MB buffer
        scan_timeout: Some(Duration::from_secs(10)),
    };

    let start = Instant::now();

    let wal = Wal::new(temp_file.path(), runtime, wal_config).await?;

    // Write multiple entries in batches
    let entries: Vec<Bytes> = (0..1000)
        .map(|i| Bytes::from(format!("WAL entry {} with batch optimization", i)))
        .collect();

    // Batch append operations
    let batch_size = 50;
    for batch in entries.chunks(batch_size) {
        let sequences = wal.append_batch(batch.to_vec()).await?;
        info!(
            "Appended batch of {} entries, sequences: {:?}-{:?}",
            batch.len(),
            sequences.first(),
            sequences.last()
        );
    }

    // Ensure all data is written
    wal.flush().await?;

    let duration = start.elapsed();

    info!("WAL operations: {} entries (took {:?})", entries.len(), duration);
    info!("WAL throughput: {:.0} ops/s", entries.len() as f64 / duration.as_secs_f64());

    Ok(())
}

/// Print comprehensive performance statistics
#[cfg(feature = "metrics")]
async fn print_performance_statistics(io_engine: &rustfs_rio::IoEngine) -> Result<()> {
    info!("📊 Performance Statistics");

    let stats = io_engine.get_stats();

    info!("I/O Engine Statistics:");
    info!("  Total operations: {}", stats.operations_total);
    info!("  Bytes transferred: {} MB", stats.bytes_transferred / (1024 * 1024));
    info!("  Batch queue depth: {}", stats.batch_queue_depth);
    info!("  Runtime type: {:?}", stats.runtime_type);
    info!("  Zero-copy enabled: {}", stats.zero_copy_enabled);

    Ok(())
}

#[cfg(not(feature = "metrics"))]
async fn print_performance_statistics(_io_engine: &rustfs_rio::IoEngine) -> Result<()> {
    info!("📊 Performance Statistics: Enable 'metrics' feature for detailed stats");
    Ok(())
}

/// Generate large test object with varied data
fn generate_large_object(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| {
            // Create varied data patterns for realistic testing
            match i % 4 {
                0 => (i % 256) as u8,
                1 => ((i * 13) % 256) as u8,
                2 => ((i * 17) % 256) as u8,
                3 => ((i * 19) % 256) as u8,
                _ => unreachable!(),
            }
        })
        .collect()
}

/// Generate compressible test object
fn generate_compressible_object(size: usize) -> Vec<u8> {
    let patterns = [
        b"RustFS high-performance distributed object storage ",
        b"with io_uring optimization and zero-copy operations ",
        b"for maximum throughput and minimal latency in S3-compatible systems ",
        b"supporting AI/ML, big data, and secure storage workloads ",
    ];

    let mut data = Vec::with_capacity(size);
    let mut pattern_idx = 0;

    while data.len() < size {
        let pattern = patterns[pattern_idx % patterns.len()];
        let remaining = size - data.len();
        let to_add = std::cmp::min(pattern.len(), remaining);

        data.extend_from_slice(&pattern[..to_add]);
        pattern_idx += 1;
    }

    data
}
