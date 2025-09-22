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

//! Advanced Optimizations Demonstration
//!
//! This example demonstrates the advanced optimizations implemented in RustFS Rio,
//! including buffer pool management, predictive optimization, and performance monitoring.

use rustfs_rio::{
    init_runtime_with_advanced_config, AdvancedRioConfig, MemoryStrategy,
    PerformanceMonitorConfig, PredictiveOptimizerConfig,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("🚀 Starting RustFS Rio Advanced Optimizations Demo");

    // Create advanced configuration
    let config = AdvancedRioConfig {
        enable_buffer_pools: true,
        enable_predictive_prefetching: true,
        enable_adaptive_batching: true,
        numa_aware: false, // Keep false for compatibility in demo
        enable_performance_monitoring: true,
        enable_ml_optimizations: true,
        memory_strategy: MemoryStrategy::Adaptive {
            min_size: 32 * 1024,    // 32KB
            max_size: 512 * 1024,   // 512KB
            max_buffers: 100,       // Smaller for demo
            growth_factor: 1.3,
        },
        performance_config: PerformanceMonitorConfig {
            metrics_collection_interval: Duration::from_secs(2),
            ..Default::default()
        },
        predictive_config: PredictiveOptimizerConfig {
            optimization_update_interval: Duration::from_secs(10),
            ..Default::default()
        },
    };

    info!("📋 Configuration: {:?}", config);

    // Initialize the enhanced runtime
    let runtime = init_runtime_with_advanced_config(config).await?;
    info!("✅ Enhanced Rio runtime initialized successfully");

    // Demonstrate buffer pool optimization
    demonstrate_buffer_pool_optimization(&runtime).await?;

    // Demonstrate performance monitoring
    demonstrate_performance_monitoring(&runtime).await?;

    // Demonstrate predictive optimization
    demonstrate_predictive_optimization(&runtime).await?;

    // Show optimization recommendations
    show_optimization_recommendations(&runtime).await?;

    info!("🎉 Advanced optimizations demonstration completed successfully");
    Ok(())
}

/// Demonstrate advanced buffer pool optimization
async fn demonstrate_buffer_pool_optimization(runtime: &rustfs_rio::EnhancedRioRuntime) -> anyhow::Result<()> {
    info!("🧠 Demonstrating Advanced Buffer Pool Optimization");

    let pool = runtime.buffer_pool().unwrap();

    // Simulate various buffer allocation patterns
    let test_cases = [
        ("Small buffers", 1024, 20),
        ("Medium buffers", 64 * 1024, 15),
        ("Large buffers", 256 * 1024, 10),
        ("Mixed sizes", 32 * 1024, 25),
    ];

    for (test_name, base_size, count) in test_cases {
        info!("  📊 Testing {}: {} buffers of ~{} bytes", test_name, count, base_size);
        
        let start_time = Instant::now();
        let mut buffers = Vec::new();

        // Allocate buffers with some size variation
        for i in 0..count {
            let size_variation = (i % 5) * 1024; // Add some variation
            let size = base_size + size_variation;
            
            let buffer = pool.get_optimized_buffer(size).await?;
            buffers.push(buffer);
        }

        let allocation_time = start_time.elapsed();
        
        // Get pool statistics
        let stats = pool.get_stats().await;
        let cache_hit_rate = if stats.total_allocations > 0 {
            (stats.cache_hits as f64 / stats.total_allocations as f64) * 100.0
        } else {
            0.0
        };

        info!("    ⚡ Allocated {} buffers in {:?}", count, allocation_time);
        info!("    📈 Cache hit rate: {:.1}%", cache_hit_rate);
        info!("    💾 Memory usage: {} bytes", stats.memory_usage_bytes);
        info!("    🎯 Average buffer size: {} bytes", stats.average_buffer_size);

        // Drop buffers to return them to pool
        drop(buffers);
        sleep(Duration::from_millis(100)).await; // Allow async return
    }

    info!("  ✅ Buffer pool optimization demonstration completed");
    Ok(())
}

/// Demonstrate performance monitoring capabilities
async fn demonstrate_performance_monitoring(runtime: &rustfs_rio::EnhancedRioRuntime) -> anyhow::Result<()> {
    info!("📊 Demonstrating Advanced Performance Monitoring");

    if let Some(monitor) = runtime.performance_monitor() {
        // Simulate some I/O operations with varying performance
        let operations = [
            ("read_small", 100.0, 10_000_000.0),    // 100μs, 10MB/s
            ("read_large", 500.0, 50_000_000.0),    // 500μs, 50MB/s  
            ("write_small", 200.0, 8_000_000.0),    // 200μs, 8MB/s
            ("write_large", 800.0, 40_000_000.0),   // 800μs, 40MB/s
            ("compress", 1200.0, 20_000_000.0),     // 1.2ms, 20MB/s
            ("encrypt", 300.0, 15_000_000.0),       // 300μs, 15MB/s
        ];

        for (op_name, base_latency, base_throughput) in operations {
            info!("  🔄 Simulating {} operations", op_name);
            
            // Simulate 10 operations with some variance
            for i in 0..10 {
                let latency_variance = (i as f64 * 10.0) - 50.0; // ±50μs variance
                let throughput_variance = (i as f64 * 1_000_000.0) - 5_000_000.0; // ±5MB/s variance
                
                let latency = (base_latency + latency_variance).max(10.0);
                let throughput = (base_throughput + throughput_variance).max(1_000_000.0);
                
                runtime.record_performance(op_name, latency, throughput).await;
                sleep(Duration::from_millis(50)).await;
            }
        }

        // Wait a moment for monitoring to process
        sleep(Duration::from_secs(3)).await;

        // Get real-time statistics
        let stats = monitor.get_real_time_stats().await;
        info!("  📈 Current Performance Stats:");
        info!("    ⚡ IOPS: {:.1}", stats.iops);
        info!("    ⏱️  Average Latency: {:.1}μs", stats.avg_latency_us);
        info!("    🚀 Throughput: {:.1} MB/s", stats.throughput_bps / 1_000_000.0);
        info!("    🎯 Cache Hit Rate: {:.1}%", stats.cache_hit_rate * 100.0);

        // Check for performance alerts
        let alerts = monitor.get_active_alerts().await;
        if !alerts.is_empty() {
            warn!("  ⚠️  Active Performance Alerts:");
            for alert in alerts {
                warn!("    🚨 {}: {} (current: {:.2}, threshold: {:.2})", 
                      alert.severity,
                      alert.message, 
                      alert.current_value,
                      alert.threshold_value);
            }
        }

        // Get performance trends
        let trends = monitor.get_performance_trends().await;
        if !trends.is_empty() {
            info!("  📊 Performance Trends:");
            for (metric, trend) in trends {
                info!("    📈 {}: {:?} (strength: {:.2}, confidence: {:.2})", 
                      metric, 
                      trend.trend_direction,
                      trend.trend_strength,
                      trend.prediction_confidence);
            }
        }
    }

    info!("  ✅ Performance monitoring demonstration completed");
    Ok(())
}

/// Demonstrate predictive optimization capabilities  
async fn demonstrate_predictive_optimization(runtime: &rustfs_rio::EnhancedRioRuntime) -> anyhow::Result<()> {
    info!("🤖 Demonstrating Predictive Optimization");

    if let Some(optimizer) = runtime.predictive_optimizer() {
        // Simulate different access patterns
        info!("  📊 Simulating access patterns for learning");

        // Sequential access pattern
        info!("    🔄 Sequential access pattern");
        for i in 0..50 {
            let offset = i * 4096; // 4KB blocks
            runtime.record_access("sequential_read", offset, 4096).await;
            runtime.record_performance("sequential_read", 150.0 + (i as f64), 30_000_000.0).await;
            sleep(Duration::from_millis(20)).await;
        }

        // Random access pattern  
        info!("    🎲 Random access pattern");
        for i in 0..30 {
            let offset = (i * 7919) % (1024 * 1024); // Pseudo-random offsets
            runtime.record_access("random_read", offset, 4096).await;
            runtime.record_performance("random_read", 400.0 + (i as f64 * 5.0), 10_000_000.0).await;
            sleep(Duration::from_millis(30)).await;
        }

        // Burst access pattern
        info!("    💥 Burst access pattern");
        for burst in 0..3 {
            // Burst of 10 operations
            for i in 0..10 {
                let offset = burst * 1024 * 1024 + i * 8192;
                runtime.record_access("burst_write", offset, 8192).await;
                runtime.record_performance("burst_write", 200.0 + (i as f64 * 2.0), 25_000_000.0).await;
                sleep(Duration::from_millis(10)).await;
            }
            
            // Inter-burst delay
            sleep(Duration::from_millis(200)).await;
        }

        // Wait for pattern analysis
        sleep(Duration::from_secs(5)).await;

        // Get detected patterns
        let patterns = optimizer.get_access_patterns().await;
        info!("  🧠 Detected Access Patterns:");
        for (operation, pattern) in patterns {
            info!("    📊 {}: {:?} (confidence: {:.2})", 
                  operation, 
                  pattern.pattern_type,
                  pattern.confidence);
        }

        // Get current optimization parameters
        let params = optimizer.get_current_parameters().await;
        info!("  ⚙️  Current Optimization Parameters:");
        info!("    🔮 Prefetch: distance={}, size={} bytes", params.prefetch_distance, params.prefetch_size);
        info!("    📦 Batch: size={}, timeout={:.1}ms", params.batch_size, params.batch_timeout_ms);
        info!("    🗜️  Compression: {} level {}", params.compression_algorithm, params.compression_level);
        info!("    🔄 Concurrency: {} ops, queue depth {}", params.max_concurrent_ops, params.queue_depth);

        // Get optimization recommendations
        let recommendations = optimizer.get_recommendations().await;
        if !recommendations.is_empty() {
            info!("  💡 Optimization Recommendations:");
            for (i, rec) in recommendations.iter().take(3).enumerate() {
                info!("    {}. {:?} (confidence: {:.2}, priority: {}, impact: {:.1}%)", 
                      i + 1,
                      rec.optimization_type,
                      rec.confidence,
                      rec.priority,
                      rec.expected_improvement * 100.0);
            }

            // Apply the top recommendation
            if let Some(top_rec) = recommendations.first() {
                info!("  🎯 Applying top recommendation...");
                if let Err(e) = optimizer.apply_optimization(top_rec.clone()).await {
                    warn!("    ⚠️  Failed to apply optimization: {}", e);
                } else {
                    info!("    ✅ Optimization applied successfully");
                    
                    // Show updated parameters
                    let new_params = optimizer.get_current_parameters().await;
                    info!("    📊 Updated parameters: batch_size={}, prefetch_distance={}", 
                          new_params.batch_size, new_params.prefetch_distance);
                }
            }
        } else {
            info!("  ℹ️  No recommendations available yet (need more data)");
        }
    }

    info!("  ✅ Predictive optimization demonstration completed");
    Ok(())
}

/// Show optimization recommendations from all components
async fn show_optimization_recommendations(runtime: &rustfs_rio::EnhancedRioRuntime) -> anyhow::Result<()> {
    info!("💡 Gathering All Optimization Recommendations");

    let recommendations = runtime.get_optimization_recommendations().await;
    
    if recommendations.is_empty() {
        info!("  ℹ️  No optimization recommendations available at this time");
    } else {
        info!("  🎯 Current Recommendations:");
        for (i, recommendation) in recommendations.iter().enumerate() {
            info!("    {}. {}", i + 1, recommendation);
        }
    }

    // Show configuration effectiveness
    let config = runtime.config();
    info!("  ⚙️  Configuration Effectiveness:");
    info!("    🧠 Buffer Pools: {}", if config.enable_buffer_pools { "✅ Enabled" } else { "❌ Disabled" });
    info!("    🔮 Predictive Prefetch: {}", if config.enable_predictive_prefetching { "✅ Enabled" } else { "❌ Disabled" });
    info!("    📦 Adaptive Batching: {}", if config.enable_adaptive_batching { "✅ Enabled" } else { "❌ Disabled" });
    info!("    📊 Performance Monitoring: {}", if config.enable_performance_monitoring { "✅ Enabled" } else { "❌ Disabled" });
    info!("    🤖 ML Optimizations: {}", if config.enable_ml_optimizations { "✅ Enabled" } else { "❌ Disabled" });

    info!("  ✅ Optimization recommendations review completed");
    Ok(())
}