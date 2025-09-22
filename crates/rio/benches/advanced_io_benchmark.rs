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

//! Advanced I/O benchmarks demonstrating io_uring optimization and reader pipeline performance

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rustfs_rio::{
    AsyncFile, CompressReader, DiskFile, EncryptReader, EtagReader, RuntimeConfig, RuntimeType, WarpReader,
    init_runtime_with_config,
};
use rustfs_utils::compress::CompressionAlgorithm;
use std::io::Cursor;
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;

/// Benchmark configuration for different scenarios
struct BenchmarkConfig {
    name: &'static str,
    data_size: usize,
    block_size: usize,
    concurrent_ops: usize,
}

impl BenchmarkConfig {
    const SMALL_FILE: BenchmarkConfig = BenchmarkConfig {
        name: "small_file",
        data_size: 64 * 1024, // 64KB
        block_size: 4 * 1024, // 4KB blocks
        concurrent_ops: 10,
    };

    const MEDIUM_FILE: BenchmarkConfig = BenchmarkConfig {
        name: "medium_file",
        data_size: 10 * 1024 * 1024, // 10MB
        block_size: 64 * 1024,       // 64KB blocks
        concurrent_ops: 32,
    };

    const LARGE_FILE: BenchmarkConfig = BenchmarkConfig {
        name: "large_file",
        data_size: 100 * 1024 * 1024, // 100MB
        block_size: 1024 * 1024,      // 1MB blocks
        concurrent_ops: 64,
    };
}

/// Benchmark enhanced EtagReader with batch MD5 processing
fn bench_etag_reader_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("etag_reader_enhanced");

    for config in [
        BenchmarkConfig::SMALL_FILE,
        BenchmarkConfig::MEDIUM_FILE,
        BenchmarkConfig::LARGE_FILE,
    ] {
        let data = generate_test_data(config.data_size);

        group.throughput(Throughput::Bytes(config.data_size as u64));

        // Benchmark original implementation (baseline)
        group.bench_with_input(BenchmarkId::new("baseline", config.name), &data, |b, data| {
            b.to_async(&rt).iter(|| async {
                let reader = Cursor::new(data);
                let warp_reader = Box::new(WarpReader::new(reader));
                let mut etag_reader = EtagReader::new(warp_reader, None);

                let mut buffer = Vec::new();
                let _ = etag_reader.read_to_end(&mut buffer).await;
                let _ = etag_reader.get_etag();

                black_box(buffer);
            });
        });

        // Benchmark enhanced implementation with optimized buffering
        group.bench_with_input(BenchmarkId::new("enhanced", config.name), &data, |b, data| {
            b.to_async(&rt).iter(|| async {
                let reader = Cursor::new(data);
                let warp_reader = Box::new(WarpReader::new(reader));
                let mut etag_reader = EtagReader::with_buffer_size(warp_reader, None, config.block_size);

                let mut buffer = Vec::new();
                let _ = etag_reader.read_to_end(&mut buffer).await;
                let _ = etag_reader.get_etag();

                black_box(buffer);
            });
        });
    }

    group.finish();
}

/// Benchmark enhanced CompressReader with batch compression
fn bench_compress_reader_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("compress_reader_enhanced");

    for config in [BenchmarkConfig::MEDIUM_FILE, BenchmarkConfig::LARGE_FILE] {
        let data = generate_compressible_data(config.data_size);

        group.throughput(Throughput::Bytes(config.data_size as u64));

        // Benchmark with different compression algorithms
        for algorithm in [CompressionAlgorithm::Deflate, CompressionAlgorithm::Gzip] {
            group.bench_with_input(BenchmarkId::new(format!("{:?}_{}", algorithm, config.name), &data), &data, |b, data| {
                b.to_async(&rt).iter(|| async {
                    let reader = Cursor::new(data);
                    let warp_reader = WarpReader::new(reader);
                    let mut compress_reader = CompressReader::with_block_size(warp_reader, config.block_size, algorithm);

                    let mut buffer = Vec::new();
                    let _ = compress_reader.read_to_end(&mut buffer).await;

                    black_box(buffer);
                });
            });
        }
    }

    group.finish();
}

/// Benchmark enhanced EncryptReader with batch encryption
fn bench_encrypt_reader_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("encrypt_reader_enhanced");

    for config in [BenchmarkConfig::MEDIUM_FILE, BenchmarkConfig::LARGE_FILE] {
        let data = generate_test_data(config.data_size);

        group.throughput(Throughput::Bytes(config.data_size as u64));

        group.bench_with_input(BenchmarkId::new("aes256_gcm", config.name), &data, |b, data| {
            b.to_async(&rt).iter(|| async {
                let reader = Cursor::new(data);
                let warp_reader = WarpReader::new(reader);
                let key = [42u8; 32]; // Test key
                let nonce = [24u8; 12]; // Test nonce

                let mut encrypt_reader = EncryptReader::with_block_size(warp_reader, key, nonce, config.block_size);

                let mut buffer = Vec::new();
                let _ = encrypt_reader.read_to_end(&mut buffer).await;

                black_box(buffer);
            });
        });
    }

    group.finish();
}

/// Benchmark reader pipeline combinations
fn bench_reader_pipeline_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("reader_pipeline_combined");

    let config = BenchmarkConfig::LARGE_FILE;
    let data = generate_compressible_data(config.data_size);

    group.throughput(Throughput::Bytes(config.data_size as u64));

    // Benchmark individual readers
    group.bench_function("etag_only", |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Cursor::new(&data);
            let warp_reader = Box::new(WarpReader::new(reader));
            let mut etag_reader = EtagReader::with_buffer_size(warp_reader, None, config.block_size);

            let mut buffer = Vec::new();
            let _ = etag_reader.read_to_end(&mut buffer).await;
            black_box(buffer);
        });
    });

    group.bench_function("compress_only", |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Cursor::new(&data);
            let warp_reader = WarpReader::new(reader);
            let mut compress_reader =
                CompressReader::with_block_size(warp_reader, config.block_size, CompressionAlgorithm::Deflate);

            let mut buffer = Vec::new();
            let _ = compress_reader.read_to_end(&mut buffer).await;
            black_box(buffer);
        });
    });

    group.bench_function("encrypt_only", |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Cursor::new(&data);
            let warp_reader = WarpReader::new(reader);
            let key = [1u8; 32];
            let nonce = [2u8; 12];
            let mut encrypt_reader = EncryptReader::with_block_size(warp_reader, key, nonce, config.block_size);

            let mut buffer = Vec::new();
            let _ = encrypt_reader.read_to_end(&mut buffer).await;
            black_box(buffer);
        });
    });

    // Benchmark combined pipeline: Compress -> Encrypt -> ETag
    group.bench_function("full_pipeline", |b| {
        b.to_async(&rt).iter(|| async {
            let reader = Cursor::new(&data);
            let warp_reader = WarpReader::new(reader);

            // Compression layer
            let compress_reader = CompressReader::with_block_size(warp_reader, config.block_size, CompressionAlgorithm::Deflate);

            // Encryption layer
            let key = [1u8; 32];
            let nonce = [2u8; 12];
            let encrypt_reader = EncryptReader::with_block_size(compress_reader, key, nonce, config.block_size);

            // ETag layer
            let mut etag_reader = EtagReader::with_buffer_size(Box::new(encrypt_reader), None, config.block_size);

            let mut buffer = Vec::new();
            let _ = etag_reader.read_to_end(&mut buffer).await;
            let _ = etag_reader.get_etag();

            black_box(buffer);
        });
    });

    group.finish();
}

/// Benchmark io_uring runtime vs Tokio runtime
fn bench_runtime_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("runtime_comparison");

    let config = BenchmarkConfig::LARGE_FILE;
    let data = generate_test_data(config.data_size);

    group.throughput(Throughput::Bytes(config.data_size as u64));

    // Benchmark Tokio runtime
    group.bench_function("tokio_runtime", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime_config = RuntimeConfig {
                runtime_type: RuntimeType::Tokio,
                ..Default::default()
            };
            let runtime = init_runtime_with_config(runtime_config).unwrap();
            let temp_file = NamedTempFile::new().unwrap();

            let mut file = DiskFile::create(temp_file.path(), runtime).await.unwrap();
            let _ = file.write_all(&data).await;
            let _ = file.sync_all().await;

            black_box(());
        });
    });

    #[cfg(feature = "io_uring")]
    group.bench_function("io_uring_runtime", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime_config = RuntimeConfig {
                runtime_type: RuntimeType::Monoio,
                ..Default::default()
            };
            // Note: This would use io_uring in a complete implementation
            let runtime = init_runtime_with_config(runtime_config).unwrap();
            let temp_file = NamedTempFile::new().unwrap();

            let mut file = DiskFile::create(temp_file.path(), runtime).await.unwrap();
            let _ = file.write_all(&data).await;
            let _ = file.sync_all().await;

            black_box(());
        });
    });

    group.finish();
}

/// Benchmark concurrent operations scaling
fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_operations");

    let data = generate_test_data(1024 * 1024); // 1MB per operation

    for concurrent_ops in [1, 4, 16, 64, 256] {
        group.throughput(Throughput::Bytes((data.len() * concurrent_ops) as u64));

        group.bench_with_input(BenchmarkId::new("concurrent_etag", concurrent_ops), &concurrent_ops, |b, &ops| {
            b.to_async(&rt).iter(|| async {
                let tasks = (0..ops)
                    .map(|_| {
                        let data_clone = data.clone();
                        tokio::spawn(async move {
                            let reader = Cursor::new(&data_clone);
                            let warp_reader = Box::new(WarpReader::new(reader));
                            let mut etag_reader = EtagReader::with_buffer_size(warp_reader, None, 64 * 1024);

                            let mut buffer = Vec::new();
                            let _ = etag_reader.read_to_end(&mut buffer).await;
                            let _ = etag_reader.get_etag();

                            buffer.len()
                        })
                    })
                    .collect::<Vec<_>>();

                let results = futures::future::join_all(tasks).await;
                black_box(results);
            });
        });
    }

    group.finish();
}

/// Generate test data with specified size
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Generate compressible test data
fn generate_compressible_data(size: usize) -> Vec<u8> {
    let pattern = b"RustFS high-performance distributed object storage with io_uring optimization ";
    let mut data = Vec::with_capacity(size);

    while data.len() < size {
        let remaining = size - data.len();
        let to_add = std::cmp::min(pattern.len(), remaining);
        data.extend_from_slice(&pattern[..to_add]);
    }

    data
}

criterion_group!(
    benches,
    bench_etag_reader_performance,
    bench_compress_reader_performance,
    bench_encrypt_reader_performance,
    bench_reader_pipeline_performance,
    bench_runtime_performance,
    bench_concurrent_operations
);

criterion_main!(benches);
