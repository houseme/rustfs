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

//! Performance demonstration comparing standard Tokio vs Rio enhanced I/O

use rustfs_rio::{init_runtime, DiskFile, AsyncFile};
use std::time::Instant;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

const DATA_SIZE: usize = 1024 * 1024; // 1MB
const NUM_OPERATIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RustFS Rio Performance Demonstration");
    println!("=========================================\n");

    // Prepare test data
    let test_data = vec![42u8; DATA_SIZE];
    
    // Test 1: Standard Tokio I/O
    println!("📊 Test 1: Standard Tokio File I/O");
    let tokio_duration = benchmark_tokio(&test_data).await?;
    
    // Test 2: Rio Enhanced I/O
    println!("📊 Test 2: Rio Enhanced File I/O");
    let rio_duration = benchmark_rio(&test_data).await?;
    
    // Calculate improvements
    let throughput_tokio = calculate_throughput(tokio_duration, DATA_SIZE * NUM_OPERATIONS);
    let throughput_rio = calculate_throughput(rio_duration, DATA_SIZE * NUM_OPERATIONS);
    let improvement = throughput_rio / throughput_tokio;
    
    println!("\n🎯 Performance Summary");
    println!("====================");
    println!("Data size per operation: {} KB", DATA_SIZE / 1024);
    println!("Number of operations: {}", NUM_OPERATIONS);
    println!("Total data processed: {} MB", (DATA_SIZE * NUM_OPERATIONS) / 1024 / 1024);
    
    println!("\n📈 Results:");
    println!("Tokio I/O:");
    println!("  Duration: {:?}", tokio_duration);
    println!("  Throughput: {:.2} MB/s", throughput_tokio);
    
    println!("Rio Enhanced I/O:");
    println!("  Duration: {:?}", rio_duration);
    println!("  Throughput: {:.2} MB/s", throughput_rio);
    
    println!("\n🚀 Performance Improvement:");
    println!("  Speed increase: {:.1}x faster", improvement);
    println!("  Latency reduction: {:.1}%", ((tokio_duration.as_secs_f64() - rio_duration.as_secs_f64()) / tokio_duration.as_secs_f64()) * 100.0);
    
    if improvement > 1.0 {
        println!("  ✅ Rio enhanced I/O shows performance improvement!");
    } else {
        println!("  ℹ️  Results may vary based on system configuration and available features");
    }
    
    println!("\n💡 Note: Maximum performance gains require:");
    println!("  - Linux kernel 5.1+ with io_uring support");
    println!("  - NVMe or high-performance SSD storage");
    println!("  - Enable 'io_uring' feature flag for zero-copy operations");
    
    Ok(())
}

async fn benchmark_tokio(test_data: &[u8]) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let start = Instant::now();
    
    for i in 0..NUM_OPERATIONS {
        let path = temp_dir.path().join(format!("tokio_test_{}.dat", i));
        
        // Write data
        let mut file = tokio::fs::File::create(&path).await?;
        file.write_all(test_data).await?;
        file.sync_all().await?;
        drop(file);
        
        // Read data back
        let mut file = tokio::fs::File::open(&path).await?;
        let mut buffer = vec![0u8; test_data.len()];
        file.read_exact(&mut buffer).await?;
        drop(file);
        
        // Verify data integrity
        assert_eq!(buffer, test_data);
    }
    
    Ok(start.elapsed())
}

async fn benchmark_rio(test_data: &[u8]) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    let runtime = init_runtime()?;
    let temp_dir = tempfile::tempdir()?;
    let start = Instant::now();
    
    for i in 0..NUM_OPERATIONS {
        let path = temp_dir.path().join(format!("rio_test_{}.dat", i));
        
        // Write data with Rio
        let mut file = DiskFile::create(&path, runtime.clone()).await?;
        let written = file.write_object(test_data, 0).await?;
        assert_eq!(written, test_data.len());
        file.sync_all().await?;
        drop(file);
        
        // Read data back with Rio
        let mut file = DiskFile::open(&path, runtime.clone()).await?;
        let mut buffer = vec![0u8; test_data.len()];
        let read = file.read_object(&mut buffer, 0).await?;
        assert_eq!(read, test_data.len());
        drop(file);
        
        // Verify data integrity
        assert_eq!(buffer, test_data);
    }
    
    Ok(start.elapsed())
}

fn calculate_throughput(duration: std::time::Duration, total_bytes: usize) -> f64 {
    let seconds = duration.as_secs_f64();
    let mb = total_bytes as f64 / 1024.0 / 1024.0;
    mb / seconds
}