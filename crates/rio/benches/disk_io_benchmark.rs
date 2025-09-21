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

//! Comprehensive benchmarks for RustFS Rio disk I/O performance
//!
//! These benchmarks compare the performance of different I/O implementations:
//! - Standard Tokio file operations
//! - Enhanced Rio with runtime abstraction
//! - io_uring-optimized operations (when available)
//! - WAL batch operations

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rustfs_rio::{init_runtime, init_runtime_with_config, DiskFile, RuntimeConfig, RuntimeType, Wal, WalConfig};
use std::io::Write;
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;

const KB: usize = 1024;
const MB: usize = 1024 * KB;

/// Benchmark data sizes for testing different scenarios
const BENCHMARK_SIZES: &[usize] = &[
    4 * KB,     // Small objects (common in object storage)
    64 * KB,    // Medium objects
    1 * MB,     // Large objects
    16 * MB,    // Very large objects
];

/// Number of concurrent operations for concurrency tests
const CONCURRENCY_LEVELS: &[usize] = &[1, 4, 16, 64];

fn setup_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .max_blocking_threads(512)
        .enable_all()
        .build()
        .unwrap()
}

/// Benchmark standard Tokio file operations (baseline)
fn bench_tokio_file_ops(c: &mut Criterion) {
    let rt = setup_runtime();
    let mut group = c.benchmark_group("tokio_file_ops");
    
    for &size in BENCHMARK_SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        
        // Write benchmark
        group.bench_with_input(
            BenchmarkId::new("write", size),
            &size,
            |b, &size| {
                let data = vec![0u8; size];
                b.to_async(&rt).iter(|| async {
                    let temp_file = NamedTempFile::new().unwrap();
                    let mut file = tokio::fs::File::create(temp_file.path()).await.unwrap();
                    let written = file.write(&data).await.unwrap();
                    file.sync_all().await.unwrap();
                    black_box(written);
                });
            },
        );
        
        // Read benchmark
        group.bench_with_input(
            BenchmarkId::new("read", size),
            &size,
            |b, &size| {
                let data = vec![42u8; size];
                b.to_async(&rt).iter(|| async {
                    // Setup: create file with data
                    let temp_file = NamedTempFile::new().unwrap();
                    let mut setup_file = std::fs::File::create(temp_file.path()).unwrap();
                    setup_file.write_all(&data).unwrap();
                    setup_file.sync_all().unwrap();
                    drop(setup_file);
                    
                    // Benchmark: read the data
                    let mut file = tokio::fs::File::open(temp_file.path()).await.unwrap();
                    let mut buf = vec![0u8; size];
                    let read = file.read(&mut buf).await.unwrap();
                    black_box(read);
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark Rio enhanced file operations
fn bench_rio_file_ops(c: &mut Criterion) {
    let rt = setup_runtime();
    let mut group = c.benchmark_group("rio_file_ops");
    
    for &size in BENCHMARK_SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        
        // Write benchmark
        group.bench_with_input(
            BenchmarkId::new("write", size),
            &size,
            |b, &size| {
                let data = vec![0u8; size];
                b.to_async(&rt).iter(|| async {
                    let runtime = init_runtime();
                    let temp_file = NamedTempFile::new().unwrap();
                    let mut file = DiskFile::create(temp_file.path(), runtime).await.unwrap();
                    let written = file.write_object(&data, 0).await.unwrap();
                    file.sync_all().await.unwrap();
                    black_box(written);
                });
            },
        );
        
        // Read benchmark
        group.bench_with_input(
            BenchmarkId::new("read", size),
            &size,
            |b, &size| {
                let data = vec![42u8; size];
                b.to_async(&rt).iter(|| async {
                    // Setup: create file with data
                    let runtime = init_runtime();
                    let temp_file = NamedTempFile::new().unwrap();
                    let mut file = DiskFile::create(temp_file.path(), runtime.clone()).await.unwrap();
                    file.write_object(&data, 0).await.unwrap();
                    file.sync_all().await.unwrap();
                    drop(file);
                    
                    // Benchmark: read the data
                    let mut file = DiskFile::open(temp_file.path(), runtime).await.unwrap();
                    let mut buf = vec![0u8; size];
                    let read = file.read_object(&mut buf, 0).await.unwrap();
                    black_box(read);
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark different runtime configurations
fn bench_runtime_types(c: &mut Criterion) {
    let rt = setup_runtime();
    let mut group = c.benchmark_group("runtime_types");
    
    let size = 64 * KB; // Medium-sized object for this test
    let data = vec![42u8; size];
    
    group.throughput(Throughput::Bytes(size as u64));
    
    // Tokio runtime
    group.bench_function("tokio_runtime", |b| {
        b.to_async(&rt).iter(|| async {
            let config = RuntimeConfig {
                runtime_type: RuntimeType::Tokio,
                ..Default::default()
            };
            let runtime = init_runtime_with_config(config);
            let temp_file = NamedTempFile::new().unwrap();
            let mut file = DiskFile::create(temp_file.path(), runtime).await.unwrap();
            let written = file.write_object(&data, 0).await.unwrap();
            black_box(written);
        });
    });
    
    #[cfg(feature = "io_uring")]
    {
        // io_uring runtime (if available)
        group.bench_function("monoio_runtime", |b| {
            b.to_async(&rt).iter(|| async {
                let config = RuntimeConfig {
                    runtime_type: RuntimeType::Monoio,
                    ..Default::default()
                };
                let runtime = init_runtime_with_config(config);
                let temp_file = NamedTempFile::new().unwrap();
                let mut file = DiskFile::create(temp_file.path(), runtime).await.unwrap();
                let written = file.write_object(&data, 0).await.unwrap();
                black_box(written);
            });
        });
    }
    
    group.finish();
}

/// Benchmark concurrent file operations
fn bench_concurrent_ops(c: &mut Criterion) {
    let rt = setup_runtime();
    let mut group = c.benchmark_group("concurrent_ops");
    
    let size = 64 * KB;
    let data = Arc::new(vec![42u8; size]);
    
    for &concurrency in CONCURRENCY_LEVELS {
        group.throughput(Throughput::Bytes((size * concurrency) as u64));
        
        // Tokio concurrent writes
        group.bench_with_input(
            BenchmarkId::new("tokio_concurrent_writes", concurrency),
            &concurrency,
            |b, &concurrency| {
                let data = data.clone();
                b.to_async(&rt).iter(|| async {
                    let temp_dir = TempDir::new().unwrap();
                    let tasks: Vec<_> = (0..concurrency)
                        .map(|i| {
                            let data = data.clone();
                            let temp_dir = temp_dir.path().to_owned();
                            tokio::spawn(async move {
                                let path = temp_dir.join(format!("file_{}.dat", i));
                                let mut file = tokio::fs::File::create(&path).await.unwrap();
                                let written = file.write(&data).await.unwrap();
                                file.sync_all().await.unwrap();
                                written
                            })
                        })
                        .collect();
                    
                    let results = futures::future::join_all(tasks).await;
                    let total_written: usize = results.into_iter().map(|r| r.unwrap()).sum();
                    black_box(total_written);
                });
            },
        );
        
        // Rio concurrent writes
        group.bench_with_input(
            BenchmarkId::new("rio_concurrent_writes", concurrency),
            &concurrency,
            |b, &concurrency| {
                let data = data.clone();
                b.to_async(&rt).iter(|| async {
                    let runtime = init_runtime();
                    let temp_dir = TempDir::new().unwrap();
                    let tasks: Vec<_> = (0..concurrency)
                        .map(|i| {
                            let data = data.clone();
                            let runtime = runtime.clone();
                            let temp_dir = temp_dir.path().to_owned();
                            tokio::spawn(async move {
                                let path = temp_dir.join(format!("file_{}.dat", i));
                                let mut file = DiskFile::create(&path, runtime).await.unwrap();
                                let written = file.write_object(&data, 0).await.unwrap();
                                file.sync_all().await.unwrap();
                                written
                            })
                        })
                        .collect();
                    
                    let results = futures::future::join_all(tasks).await;
                    let total_written: usize = results.into_iter().map(|r| r.unwrap()).sum();
                    black_box(total_written);
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark WAL operations
fn bench_wal_ops(c: &mut Criterion) {
    let rt = setup_runtime();
    let mut group = c.benchmark_group("wal_ops");
    
    let entry_sizes = [256, 1024, 4096]; // Different WAL entry sizes
    
    for &entry_size in &entry_sizes {
        group.throughput(Throughput::Bytes(entry_size as u64));
        
        // Single entry append
        group.bench_with_input(
            BenchmarkId::new("single_append", entry_size),
            &entry_size,
            |b, &entry_size| {
                let data = Bytes::from(vec![42u8; entry_size]);
                b.to_async(&rt).iter(|| async {
                    let runtime = init_runtime();
                    let temp_file = NamedTempFile::new().unwrap();
                    let wal = Wal::new(temp_file.path(), runtime, WalConfig::default())
                        .await
                        .unwrap();
                    
                    let sequence = wal.append(data.clone()).await.unwrap();
                    wal.flush().await.unwrap();
                    black_box(sequence);
                });
            },
        );
        
        // Batch append
        group.bench_with_input(
            BenchmarkId::new("batch_append_100", entry_size),
            &entry_size,
            |b, &entry_size| {
                let entries: Vec<Bytes> = (0..100)
                    .map(|_| Bytes::from(vec![42u8; entry_size]))
                    .collect();
                
                b.to_async(&rt).iter(|| async {
                    let runtime = init_runtime();
                    let temp_file = NamedTempFile::new().unwrap();
                    let wal = Wal::new(temp_file.path(), runtime, WalConfig::default())
                        .await
                        .unwrap();
                    
                    let sequences = wal.append_batch(entries.clone()).await.unwrap();
                    wal.flush().await.unwrap();
                    black_box(sequences);
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark WAL configuration impacts
fn bench_wal_configs(c: &mut Criterion) {
    let rt = setup_runtime();
    let mut group = c.benchmark_group("wal_configs");
    
    let data = Bytes::from(vec![42u8; 1024]);
    let num_entries = 1000;
    
    group.throughput(Throughput::Bytes((1024 * num_entries) as u64));
    
    // Small batch size (more frequent flushes)
    group.bench_function("small_batch_10", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime = init_runtime();
            let config = WalConfig {
                batch_size: 10,
                ..Default::default()
            };
            let temp_file = NamedTempFile::new().unwrap();
            let wal = Wal::new(temp_file.path(), runtime, config).await.unwrap();
            
            for _ in 0..num_entries {
                let _ = wal.append(data.clone()).await.unwrap();
            }
            wal.flush().await.unwrap();
        });
    });
    
    // Large batch size (fewer flushes)
    group.bench_function("large_batch_1000", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime = init_runtime();
            let config = WalConfig {
                batch_size: 1000,
                ..Default::default()
            };
            let temp_file = NamedTempFile::new().unwrap();
            let wal = Wal::new(temp_file.path(), runtime, config).await.unwrap();
            
            for _ in 0..num_entries {
                let _ = wal.append(data.clone()).await.unwrap();
            }
            wal.flush().await.unwrap();
        });
    });
    
    // No sync after batch (faster but less durable)
    group.bench_function("no_sync", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime = init_runtime();
            let config = WalConfig {
                batch_size: 100,
                sync_after_batch: false,
                ..Default::default()
            };
            let temp_file = NamedTempFile::new().unwrap();
            let wal = Wal::new(temp_file.path(), runtime, config).await.unwrap();
            
            for _ in 0..num_entries {
                let _ = wal.append(data.clone()).await.unwrap();
            }
            wal.flush().await.unwrap();
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_tokio_file_ops,
    bench_rio_file_ops,
    bench_runtime_types,
    bench_concurrent_ops,
    bench_wal_ops,
    bench_wal_configs
);

criterion_main!(benches);