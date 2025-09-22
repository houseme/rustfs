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

//! High-Performance I/O Example for RustFS Rio
//!
//! This example demonstrates how to use RustFS Rio for optimized disk I/O operations
//! including runtime detection, file operations, and WAL usage.

use bytes::Bytes;
use rustfs_rio::{AsyncFile, DiskFile, RuntimeConfig, RuntimeType, Wal, WalConfig, init_runtime, init_runtime_with_config};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{Level, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for observability
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting RustFS Rio high-performance I/O example");

    // Example 1: Automatic runtime detection
    demonstrate_runtime_detection().await?;

    // Example 2: High-performance file operations
    demonstrate_file_operations().await?;

    // Example 3: Write-Ahead Log operations
    demonstrate_wal_operations().await?;

    // Example 4: Concurrent operations
    demonstrate_concurrent_operations().await?;

    info!("Completed all examples successfully!");
    Ok(())
}

/// Demonstrate automatic runtime detection and configuration
async fn demonstrate_runtime_detection() -> Result<(), Box<dyn std::error::Error>> {
    info!("=== Runtime Detection Example ===");

    // Automatic detection (recommended)
    let runtime = init_runtime();
    info!("Auto-detected runtime: {:?}", runtime.runtime_type());
    info!("Supports zero-copy: {}", runtime.supports_zero_copy());

    // Manual configuration
    let config = RuntimeConfig {
        runtime_type: RuntimeType::Tokio,
        max_blocking_threads: Some(1024),
        ..Default::default()
    };
    let runtime = init_runtime_with_config(config);
    info!("Manual runtime configuration: {:?}", runtime.runtime_type());

    #[cfg(feature = "io_uring")]
    {
        // Try io_uring runtime if available
        let config = RuntimeConfig {
            runtime_type: RuntimeType::Monoio,
            ..Default::default()
        };
        let runtime = init_runtime_with_config(config);
        info!("io_uring runtime configured: {:?}", runtime.runtime_type());
    }

    Ok(())
}

/// Demonstrate high-performance file operations
async fn demonstrate_file_operations() -> Result<(), Box<dyn std::error::Error>> {
    info!("=== File Operations Example ===");

    let runtime = init_runtime();
    let temp_dir = tempfile::tempdir()?;
    let file_path = temp_dir.path().join("example.dat");

    // Create and write to file
    let start = Instant::now();
    let mut file = DiskFile::create(&file_path, runtime.clone()).await?;

    // Write data in chunks to demonstrate performance
    let chunk_size = 64 * 1024; // 64KB chunks
    let num_chunks = 100;
    let data = vec![42u8; chunk_size];

    info!("Writing {} chunks of {} bytes each", num_chunks, chunk_size);

    for i in 0..num_chunks {
        let offset = (i * chunk_size) as u64;
        let written = file.write_object(&data, offset).await?;
        assert_eq!(written, chunk_size);
    }

    // Ensure data is written to disk
    file.sync_all().await?;
    let write_duration = start.elapsed();

    let total_bytes = num_chunks * chunk_size;
    let write_throughput = (total_bytes as f64) / write_duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
        "Write completed: {} MB in {:?} ({:.2} MB/s)",
        total_bytes / 1024 / 1024,
        write_duration,
        write_throughput
    );

    drop(file);

    // Read back the data
    let start = Instant::now();
    let mut file = DiskFile::open(&file_path, runtime).await?;

    info!("Reading back {} chunks", num_chunks);

    for i in 0..num_chunks {
        let offset = (i * chunk_size) as u64;
        let mut buffer = vec![0u8; chunk_size];
        let read = file.read_object(&mut buffer, offset).await?;
        assert_eq!(read, chunk_size);
        assert_eq!(buffer, data); // Verify data integrity
    }

    let read_duration = start.elapsed();
    let read_throughput = (total_bytes as f64) / read_duration.as_secs_f64() / 1024.0 / 1024.0;

    info!(
        "Read completed: {} MB in {:?} ({:.2} MB/s)",
        total_bytes / 1024 / 1024,
        read_duration,
        read_throughput
    );

    Ok(())
}

/// Demonstrate Write-Ahead Log operations
async fn demonstrate_wal_operations() -> Result<(), Box<dyn std::error::Error>> {
    info!("=== WAL Operations Example ===");

    let runtime = init_runtime();
    let temp_dir = tempfile::tempdir()?;
    let wal_path = temp_dir.path().join("example.wal");

    // Configure WAL for optimal performance
    let wal_config = WalConfig {
        batch_size: 50,                                     // Batch up to 50 entries
        flush_timeout: std::time::Duration::from_millis(5), // Flush after 5ms
        sync_after_batch: true,                             // Ensure durability
        buffer_size: 128 * 1024,                            // 128KB buffer
    };

    let wal = Wal::new(&wal_path, runtime, wal_config).await?;

    // Single entry operations
    info!("Testing single WAL entries");
    let start = Instant::now();

    for i in 0..100 {
        let data = Bytes::from(format!("Entry number {}", i));
        let sequence = wal.append(data).await?;
        if i == 0 {
            info!("First entry sequence: {}", sequence);
        }
    }

    wal.flush().await?;
    let single_duration = start.elapsed();
    info!("Single entries: 100 entries in {:?}", single_duration);

    // Batch operations
    info!("Testing batch WAL entries");
    let start = Instant::now();

    let batch_entries: Vec<Bytes> = (0..100).map(|i| Bytes::from(format!("Batch entry {}", i))).collect();

    let sequences = wal.append_batch(batch_entries).await?;
    wal.flush().await?;

    let batch_duration = start.elapsed();
    info!(
        "Batch entries: 100 entries in {:?} (sequences: {}-{})",
        batch_duration,
        sequences.first().unwrap(),
        sequences.last().unwrap()
    );

    // Compare performance
    let single_rate = 100.0 / single_duration.as_secs_f64();
    let batch_rate = 100.0 / batch_duration.as_secs_f64();

    info!(
        "Performance comparison: Single={:.0} entries/sec, Batch={:.0} entries/sec ({:.1}x faster)",
        single_rate,
        batch_rate,
        batch_rate / single_rate
    );

    Ok(())
}

/// Demonstrate concurrent operations for maximum throughput
async fn demonstrate_concurrent_operations() -> Result<(), Box<dyn std::error::Error>> {
    info!("=== Concurrent Operations Example ===");

    let runtime = init_runtime();
    let temp_dir = tempfile::tempdir()?;

    // Test concurrent file writes
    let num_tasks = 16;
    let writes_per_task = 50;
    let data_size = 32 * 1024; // 32KB per write

    info!(
        "Starting {} concurrent tasks, {} writes each ({} KB per write)",
        num_tasks,
        writes_per_task,
        data_size / 1024
    );

    let start = Instant::now();
    let tasks: Vec<_> = (0..num_tasks)
        .map(|task_id| {
            let runtime = runtime.clone();
            let temp_dir = temp_dir.path().to_owned();

            tokio::spawn(async move {
                let file_path = temp_dir.join(format!("concurrent_{}.dat", task_id));
                let mut file = DiskFile::create(&file_path, runtime).await?;

                let data = vec![task_id as u8; data_size];
                let mut total_written = 0;

                for write_id in 0..writes_per_task {
                    let offset = (write_id * data_size) as u64;
                    let written = file.write_object(&data, offset).await?;
                    total_written += written;
                }

                file.sync_all().await?;
                Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(total_written)
            })
        })
        .collect();

    // Wait for all tasks to complete
    let results = futures::future::join_all(tasks).await;
    let duration = start.elapsed();

    // Calculate statistics
    let mut total_bytes = 0;
    let mut successful_tasks = 0;

    for result in results {
        match result {
            Ok(Ok(bytes)) => {
                total_bytes += bytes;
                successful_tasks += 1;
            }
            Ok(Err(e)) => info!("Task failed: {}", e),
            Err(e) => info!("Task panicked: {}", e),
        }
    }

    let throughput = (total_bytes as f64) / duration.as_secs_f64() / 1024.0 / 1024.0;
    let iops = (successful_tasks * writes_per_task) as f64 / duration.as_secs_f64();

    info!(
        "Concurrent write results: {} successful tasks, {} MB total, {:.2} MB/s, {:.0} IOPS",
        successful_tasks,
        total_bytes / 1024 / 1024,
        throughput,
        iops
    );

    // Verify data integrity
    info!("Verifying data integrity...");
    for task_id in 0..successful_tasks {
        let file_path = temp_dir.path().join(format!("concurrent_{}.dat", task_id));
        let mut file = DiskFile::open(&file_path, runtime.clone()).await?;

        let expected_data = vec![task_id as u8; data_size];
        for write_id in 0..writes_per_task {
            let offset = (write_id * data_size) as u64;
            let mut buffer = vec![0u8; data_size];
            let read = file.read_object(&mut buffer, offset).await?;

            assert_eq!(read, data_size);
            assert_eq!(buffer, expected_data);
        }
    }

    info!("Data integrity verified for all {} tasks", successful_tasks);

    Ok(())
}
