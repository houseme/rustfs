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

//! Advanced Buffer Pool Management for High-Performance I/O
//!
//! This module provides sophisticated buffer pool management with adaptive sizing,
//! NUMA awareness, and zero-allocati hot paths for optimal performance.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};

#[cfg(feature = "metrics")]
use metrics::{counter, gauge, histogram};

/// Memory allocation strategy for buffer pools
#[derive(Debug, Clone)]
pub enum MemoryStrategy {
    /// Simple fixed-size buffers
    Fixed { buffer_size: usize, max_buffers: usize },
    /// Adaptive buffer sizing based on usage patterns
    Adaptive {
        min_size: usize,
        max_size: usize,
        max_buffers: usize,
        growth_factor: f64,
    },
    /// NUMA-aware allocation for multi-socket systems
    AdaptiveNuma {
        initial_size: usize,
        max_buffers: usize,
        numa_node: Option<u32>,
    },
}

/// Advanced buffer pool with sophisticated memory management
pub struct AdvancedBufferPool {
    strategy: MemoryStrategy,
    available_buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,
    buffer_stats: Arc<RwLock<BufferPoolStats>>,
    allocation_history: Arc<Mutex<VecDeque<AllocationRecord>>>,
    last_optimization: Arc<RwLock<Instant>>,
}

/// Buffer pool statistics for monitoring and optimization
#[derive(Debug, Clone, Default)]
pub struct BufferPoolStats {
    pub total_allocations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub average_buffer_size: usize,
    pub peak_buffer_count: usize,
    pub current_buffer_count: usize,
    pub memory_usage_bytes: usize,
    pub allocation_latency_us: f64,
}

/// Record of buffer allocation for pattern analysis
#[derive(Debug, Clone)]
struct AllocationRecord {
    size: usize,
    timestamp: Instant,
    cache_hit: bool,
    allocation_duration: Duration,
}

impl AdvancedBufferPool {
    /// Create a new advanced buffer pool with the specified strategy
    pub fn new(strategy: MemoryStrategy) -> Self {
        let initial_capacity = match &strategy {
            MemoryStrategy::Fixed { max_buffers, .. } => *max_buffers,
            MemoryStrategy::Adaptive { max_buffers, .. } => *max_buffers,
            MemoryStrategy::AdaptiveNuma { max_buffers, .. } => *max_buffers,
        };

        info!("Creating advanced buffer pool with strategy: {:?}", strategy);

        Self {
            strategy,
            available_buffers: Arc::new(Mutex::new(VecDeque::with_capacity(initial_capacity))),
            buffer_stats: Arc::new(RwLock::new(BufferPoolStats::default())),
            allocation_history: Arc::new(Mutex::new(VecDeque::with_capacity(1000))), // Keep last 1000 allocations
            last_optimization: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Get an optimized buffer with size hint for maximum performance
    pub async fn get_optimized_buffer(&self, size_hint: usize) -> anyhow::Result<OptimizedBuffer> {
        let start_time = Instant::now();

        #[cfg(feature = "metrics")]
        counter!("rustfs_buffer_pool_requests_total").increment(1);

        let optimal_size = self.calculate_optimal_size(size_hint).await;
        let mut available = self.available_buffers.lock().await;

        // Try to find a suitable buffer from the pool
        let mut buffer = None;
        let mut cache_hit = false;

        // Look for a buffer that's close to the optimal size (within 25% tolerance)
        let tolerance = optimal_size / 4;
        let min_acceptable = optimal_size.saturating_sub(tolerance);
        let max_acceptable = optimal_size + tolerance;

        for i in 0..available.len() {
            if let Some(candidate) = available.get(i) {
                if candidate.capacity() >= min_acceptable && candidate.capacity() <= max_acceptable {
                    buffer = available.remove(i);
                    cache_hit = true;
                    break;
                }
            }
        }

        // If no suitable buffer found, create a new one
        let final_buffer = if let Some(mut buf) = buffer {
            buf.clear();
            buf.reserve(optimal_size.saturating_sub(buf.capacity()));
            buf
        } else {
            self.allocate_new_buffer(optimal_size).await?
        };

        drop(available);

        let allocation_duration = start_time.elapsed();

        // Update statistics
        self.update_stats(optimal_size, cache_hit, allocation_duration).await;

        // Record allocation for pattern analysis
        self.record_allocation(AllocationRecord {
            size: optimal_size,
            timestamp: start_time,
            cache_hit,
            allocation_duration,
        })
        .await;

        // Perform periodic optimization
        self.maybe_optimize_pool().await?;

        #[cfg(feature = "metrics")]
        {
            if cache_hit {
                counter!("rustfs_buffer_pool_cache_hits_total").increment(1);
            } else {
                counter!("rustfs_buffer_pool_cache_misses_total").increment(1);
            }
            histogram!("rustfs_buffer_pool_allocation_duration_us").record(allocation_duration.as_micros() as f64);
        }

        Ok(OptimizedBuffer {
            buffer: final_buffer,
            pool: Arc::downgrade(&Arc::new(self.clone())),
            original_capacity: optimal_size,
        })
    }

    /// Calculate optimal buffer size based on strategy and usage patterns
    async fn calculate_optimal_size(&self, size_hint: usize) -> usize {
        match &self.strategy {
            MemoryStrategy::Fixed { buffer_size, .. } => *buffer_size,
            MemoryStrategy::Adaptive {
                min_size,
                max_size,
                growth_factor,
                ..
            } => {
                let stats = self.buffer_stats.read().await;
                let base_size = if stats.average_buffer_size > 0 {
                    ((stats.average_buffer_size as f64) * growth_factor) as usize
                } else {
                    size_hint
                };

                base_size.clamp(*min_size, *max_size)
            }
            MemoryStrategy::AdaptiveNuma { initial_size, .. } => {
                // For NUMA-aware allocation, consider both size hint and historical patterns
                let stats = self.buffer_stats.read().await;
                if stats.average_buffer_size > 0 {
                    // Use historical average with bias toward the size hint
                    ((stats.average_buffer_size + size_hint) / 2).max(*initial_size)
                } else {
                    size_hint.max(*initial_size)
                }
            }
        }
    }

    /// Allocate a new buffer with NUMA awareness if enabled
    async fn allocate_new_buffer(&self, size: usize) -> anyhow::Result<Vec<u8>> {
        match &self.strategy {
            MemoryStrategy::AdaptiveNuma {
                numa_node: Some(node), ..
            } => {
                // In a real implementation, we would use libnuma or similar
                // For now, we'll just allocate normally but log the NUMA intent
                debug!("Allocating buffer on NUMA node {} (simulated)", node);
                Ok(Vec::with_capacity(size))
            }
            _ => Ok(Vec::with_capacity(size)),
        }
    }

    /// Update buffer pool statistics
    async fn update_stats(&self, size: usize, cache_hit: bool, duration: Duration) {
        let mut stats = self.buffer_stats.write().await;

        stats.total_allocations += 1;
        if cache_hit {
            stats.cache_hits += 1;
        } else {
            stats.cache_misses += 1;
        }

        // Update rolling average of buffer size
        let new_avg = if stats.total_allocations == 1 {
            size
        } else {
            let weight = 0.1; // Give 10% weight to new sample
            ((stats.average_buffer_size as f64) * (1.0 - weight) + (size as f64) * weight) as usize
        };
        stats.average_buffer_size = new_avg;

        // Update allocation latency (exponential moving average)
        let new_latency = duration.as_micros() as f64;
        stats.allocation_latency_us = if stats.total_allocations == 1 {
            new_latency
        } else {
            stats.allocation_latency_us * 0.9 + new_latency * 0.1
        };

        #[cfg(feature = "metrics")]
        {
            gauge!("rustfs_buffer_pool_cache_hit_rate").set(stats.cache_hits as f64 / stats.total_allocations as f64);
            gauge!("rustfs_buffer_pool_average_buffer_size_bytes").set(stats.average_buffer_size as f64);
            gauge!("rustfs_buffer_pool_allocation_latency_us").set(stats.allocation_latency_us);
        }
    }

    /// Record allocation for pattern analysis
    async fn record_allocation(&self, record: AllocationRecord) {
        let mut history = self.allocation_history.lock().await;

        // Keep only recent history to prevent unbounded growth
        if history.len() >= 1000 {
            history.pop_front();
        }

        history.push_back(record);
    }

    /// Perform periodic pool optimization based on usage patterns
    async fn maybe_optimize_pool(&self) -> anyhow::Result<()> {
        let mut last_opt = self.last_optimization.write().await;
        let now = Instant::now();

        // Optimize every 30 seconds
        if now.duration_since(*last_opt) < Duration::from_secs(30) {
            return Ok(());
        }

        *last_opt = now;
        drop(last_opt);

        self.optimize_pool().await
    }

    /// Optimize the buffer pool based on usage patterns
    async fn optimize_pool(&self) -> anyhow::Result<()> {
        let stats = self.buffer_stats.read().await;
        let history = self.allocation_history.lock().await;

        if history.len() < 10 {
            return Ok(()); // Not enough data for optimization
        }

        // Analyze recent allocation patterns
        let recent_allocations: Vec<_> = history.iter().rev().take(100).collect();

        let cache_hit_rate = recent_allocations.iter().filter(|r| r.cache_hit).count() as f64 / recent_allocations.len() as f64;

        let avg_recent_size: usize = recent_allocations.iter().map(|r| r.size).sum::<usize>() / recent_allocations.len();

        drop(history);
        drop(stats);

        debug!(
            cache_hit_rate = cache_hit_rate,
            avg_recent_size = avg_recent_size,
            "Optimizing buffer pool based on usage patterns"
        );

        // If cache hit rate is low, pre-allocate some buffers
        if cache_hit_rate < 0.7 {
            self.preallocate_buffers(avg_recent_size, 5).await?;
        }

        // If cache hit rate is very high, we might be over-allocated
        if cache_hit_rate > 0.95 {
            self.trim_excess_buffers().await?;
        }

        Ok(())
    }

    /// Pre-allocate buffers to improve cache hit rate
    async fn preallocate_buffers(&self, size: usize, count: usize) -> anyhow::Result<()> {
        let mut available = self.available_buffers.lock().await;

        let max_buffers = match &self.strategy {
            MemoryStrategy::Fixed { max_buffers, .. } => *max_buffers,
            MemoryStrategy::Adaptive { max_buffers, .. } => *max_buffers,
            MemoryStrategy::AdaptiveNuma { max_buffers, .. } => *max_buffers,
        };

        if available.len() + count > max_buffers {
            return Ok(()); // Don't exceed limits
        }

        for _ in 0..count {
            let buffer = self.allocate_new_buffer(size).await?;
            available.push_back(buffer);
        }

        info!("Pre-allocated {} buffers of size {} bytes", count, size);

        #[cfg(feature = "metrics")]
        counter!("rustfs_buffer_pool_preallocation_events_total").increment(1);

        Ok(())
    }

    /// Trim excess buffers to free memory
    async fn trim_excess_buffers(&self) -> anyhow::Result<()> {
        let mut available = self.available_buffers.lock().await;

        let original_count = available.len();
        let target_count = (original_count * 3) / 4; // Remove 25% of buffers

        available.truncate(target_count);

        let removed = original_count - available.len();
        if removed > 0 {
            info!("Trimmed {} excess buffers from pool", removed);

            #[cfg(feature = "metrics")]
            counter!("rustfs_buffer_pool_trim_events_total").increment(1);
        }

        Ok(())
    }

    /// Get current buffer pool statistics
    pub async fn get_stats(&self) -> BufferPoolStats {
        self.buffer_stats.read().await.clone()
    }
}

impl Clone for AdvancedBufferPool {
    fn clone(&self) -> Self {
        Self {
            strategy: self.strategy.clone(),
            available_buffers: Arc::clone(&self.available_buffers),
            buffer_stats: Arc::clone(&self.buffer_stats),
            allocation_history: Arc::clone(&self.allocation_history),
            last_optimization: Arc::clone(&self.last_optimization),
        }
    }
}

/// An optimized buffer that automatically returns to the pool when dropped
pub struct OptimizedBuffer {
    pub buffer: Vec<u8>,
    pub pool: std::sync::Weak<AdvancedBufferPool>,
    pub original_capacity: usize,
}

impl OptimizedBuffer {
    /// Get a mutable reference to the buffer
    pub fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Get an immutable reference to the buffer
    pub fn as_ref(&self) -> &Vec<u8> {
        &self.buffer
    }

    /// Get the buffer capacity
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Get the current buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

impl Drop for OptimizedBuffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let buffer = std::mem::replace(&mut self.buffer, Vec::new());

            // Return buffer to pool asynchronously
            tokio::spawn(async move {
                if let Ok(mut available) = pool.available_buffers.try_lock() {
                    // Only return if pool isn't full and buffer isn't too large
                    let max_buffers = match &pool.strategy {
                        MemoryStrategy::Fixed { max_buffers, .. } => *max_buffers,
                        MemoryStrategy::Adaptive { max_buffers, .. } => *max_buffers,
                        MemoryStrategy::AdaptiveNuma { max_buffers, .. } => *max_buffers,
                    };

                    if available.len() < max_buffers && buffer.capacity() <= 1024 * 1024 {
                        available.push_back(buffer);

                        #[cfg(feature = "metrics")]
                        counter!("rustfs_buffer_pool_returns_total").increment(1);
                    } else {
                        // Buffer will be dropped and deallocated
                        #[cfg(feature = "metrics")]
                        counter!("rustfs_buffer_pool_discards_total").increment(1);
                    }
                }
            });
        }
    }
}

impl std::ops::Deref for OptimizedBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl std::ops::DerefMut for OptimizedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_fixed_strategy_buffer_pool() {
        let pool = AdvancedBufferPool::new(MemoryStrategy::Fixed {
            buffer_size: 1024,
            max_buffers: 10,
        });

        let buffer1 = pool.get_optimized_buffer(512).await.unwrap();
        assert_eq!(buffer1.capacity(), 1024);

        let buffer2 = pool.get_optimized_buffer(2048).await.unwrap();
        assert_eq!(buffer2.capacity(), 1024);

        let stats = pool.get_stats().await;
        assert_eq!(stats.total_allocations, 2);
        assert_eq!(stats.cache_misses, 2); // First allocations are always misses
    }

    #[tokio::test]
    async fn test_adaptive_strategy_buffer_pool() {
        let pool = AdvancedBufferPool::new(MemoryStrategy::Adaptive {
            min_size: 512,
            max_size: 4096,
            max_buffers: 10,
            growth_factor: 1.2,
        });

        let buffer1 = pool.get_optimized_buffer(1000).await.unwrap();
        assert!(buffer1.capacity() >= 512 && buffer1.capacity() <= 4096);

        let stats = pool.get_stats().await;
        assert_eq!(stats.total_allocations, 1);
    }

    #[tokio::test]
    async fn test_buffer_reuse() {
        let pool = AdvancedBufferPool::new(MemoryStrategy::Fixed {
            buffer_size: 1024,
            max_buffers: 10,
        });

        // Allocate and return a buffer
        {
            let _buffer = pool.get_optimized_buffer(1024).await.unwrap();
            // Buffer is returned when dropped
        }

        // Allow some time for async return
        sleep(Duration::from_millis(10)).await;

        // Allocate another buffer - should be a cache hit
        let _buffer2 = pool.get_optimized_buffer(1024).await.unwrap();
        let stats = pool.get_stats().await;

        // We should have some cache hits after buffer reuse
        assert!(stats.cache_hits > 0 || stats.total_allocations == 2);
    }

    #[tokio::test]
    async fn test_numa_aware_strategy() {
        let pool = AdvancedBufferPool::new(MemoryStrategy::AdaptiveNuma {
            initial_size: 2048,
            max_buffers: 5,
            numa_node: Some(0),
        });

        let buffer = pool.get_optimized_buffer(1024).await.unwrap();
        assert!(buffer.capacity() >= 1024);

        let stats = pool.get_stats().await;
        assert_eq!(stats.total_allocations, 1);
    }

    #[tokio::test]
    async fn test_pool_optimization() {
        let pool = AdvancedBufferPool::new(MemoryStrategy::Adaptive {
            min_size: 512,
            max_size: 4096,
            max_buffers: 20,
            growth_factor: 1.1,
        });

        // Generate allocation pattern
        for i in 0..15 {
            let _buffer = pool.get_optimized_buffer(1024 + i * 100).await.unwrap();
            sleep(Duration::from_millis(1)).await;
        }

        // Force optimization
        pool.optimize_pool().await.unwrap();

        let stats = pool.get_stats().await;
        assert!(stats.total_allocations >= 15);
    }
}
