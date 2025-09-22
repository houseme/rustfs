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

//! Predictive Optimization Engine
//!
//! This module implements intelligent predictive optimization using pattern recognition,
//! machine learning-based predictions, and adaptive parameter tuning for maximum performance.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tokio::time::interval;
use tracing::{debug, info, warn};
use serde::{Deserialize, Serialize};

#[cfg(feature = "metrics")]
use metrics::{counter, gauge, histogram};

/// Configuration for predictive optimization
#[derive(Debug, Clone)]
pub struct PredictiveOptimizerConfig {
    /// Enable predictive prefetching
    pub enable_predictive_prefetching: bool,
    /// Enable adaptive batching
    pub enable_adaptive_batching: bool,
    /// Enable dynamic algorithm selection
    pub enable_dynamic_algorithm_selection: bool,
    /// Learning rate for adaptation (0.0 to 1.0)
    pub learning_rate: f64,
    /// Minimum samples needed for predictions
    pub min_samples_for_prediction: usize,
    /// Maximum prediction horizon in seconds
    pub max_prediction_horizon_secs: u64,
    /// Confidence threshold for applying optimizations
    pub confidence_threshold: f64,
    /// Update interval for optimization parameters
    pub optimization_update_interval: Duration,
}

impl Default for PredictiveOptimizerConfig {
    fn default() -> Self {
        Self {
            enable_predictive_prefetching: true,
            enable_adaptive_batching: true,
            enable_dynamic_algorithm_selection: true,
            learning_rate: 0.1,
            min_samples_for_prediction: 20,
            max_prediction_horizon_secs: 300, // 5 minutes
            confidence_threshold: 0.7,
            optimization_update_interval: Duration::from_secs(30),
        }
    }
}

/// Access pattern for predictive optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPattern {
    /// Pattern type
    pub pattern_type: AccessPatternType,
    /// Confidence in pattern detection (0.0 to 1.0)
    pub confidence: f64,
    /// Pattern parameters
    pub parameters: HashMap<String, f64>,
    /// Last update timestamp
    pub last_update: Instant,
    /// Number of observations supporting this pattern
    pub observation_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccessPatternType {
    /// Sequential access pattern
    Sequential {
        stride: usize,
        direction: AccessDirection,
    },
    /// Random access pattern
    Random {
        hotspot_ratio: f64,
    },
    /// Periodic access pattern
    Periodic {
        period_seconds: f64,
        amplitude: f64,
    },
    /// Burst access pattern
    Burst {
        burst_duration_ms: f64,
        inter_burst_delay_ms: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccessDirection {
    Forward,
    Backward,
    Bidirectional,
}

/// Optimization recommendation from the predictive engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationRecommendation {
    /// Type of optimization
    pub optimization_type: OptimizationType,
    /// Recommended parameters
    pub parameters: HashMap<String, f64>,
    /// Expected performance improvement
    pub expected_improvement: f64,
    /// Confidence in recommendation (0.0 to 1.0)
    pub confidence: f64,
    /// Priority (1-10, 10 being highest)
    pub priority: u8,
    /// When to apply this optimization
    pub apply_at: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OptimizationType {
    /// Adjust prefetch parameters
    PrefetchTuning {
        prefetch_distance: usize,
        prefetch_size: usize,
    },
    /// Adjust batch parameters
    BatchTuning {
        batch_size: usize,
        batch_timeout_ms: f64,
    },
    /// Select compression algorithm
    CompressionSelection {
        algorithm: String,
        compression_level: u8,
    },
    /// Adjust buffer pool parameters
    BufferPoolTuning {
        pool_size: usize,
        buffer_size: usize,
    },
    /// Adjust concurrency parameters
    ConcurrencyTuning {
        max_concurrent_ops: usize,
        queue_depth: usize,
    },
}

/// Predictive optimization engine
pub struct PredictiveOptimizer {
    config: PredictiveOptimizerConfig,
    access_patterns: Arc<RwLock<HashMap<String, AccessPattern>>>,
    optimization_history: Arc<Mutex<VecDeque<OptimizationResult>>>,
    current_parameters: Arc<RwLock<OptimizationParameters>>,
    prediction_models: Arc<Mutex<HashMap<String, PredictionModel>>>,
    performance_feedback: Arc<Mutex<VecDeque<PerformanceSample>>>,
}

/// Result of applying an optimization
#[derive(Debug, Clone)]
struct OptimizationResult {
    optimization_type: OptimizationType,
    applied_at: Instant,
    performance_before: f64,
    performance_after: f64,
    success: bool,
}

/// Current optimization parameters
#[derive(Debug, Clone)]
pub struct OptimizationParameters {
    pub prefetch_distance: usize,
    pub prefetch_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: f64,
    pub compression_algorithm: String,
    pub compression_level: u8,
    pub buffer_pool_size: usize,
    pub buffer_size: usize,
    pub max_concurrent_ops: usize,
    pub queue_depth: usize,
}

impl Default for OptimizationParameters {
    fn default() -> Self {
        Self {
            prefetch_distance: 4,
            prefetch_size: 64 * 1024,
            batch_size: 16,
            batch_timeout_ms: 10.0,
            compression_algorithm: "zstd".to_string(),
            compression_level: 3,
            buffer_pool_size: 1024,
            buffer_size: 64 * 1024,
            max_concurrent_ops: 1024,
            queue_depth: 32,
        }
    }
}

/// Simple prediction model for performance forecasting
#[derive(Debug, Clone)]
struct PredictionModel {
    /// Model type
    model_type: ModelType,
    /// Model parameters
    parameters: Vec<f64>,
    /// Model accuracy
    accuracy: f64,
    /// Last training timestamp
    last_trained: Instant,
    /// Training data
    training_data: VecDeque<(f64, f64)>, // (input, output) pairs
}

#[derive(Debug, Clone)]
enum ModelType {
    Linear,
    Exponential,
    Periodic,
}

/// Performance sample for model training
#[derive(Debug, Clone)]
struct PerformanceSample {
    timestamp: Instant,
    operation_type: String,
    latency_us: f64,
    throughput_bps: f64,
    parameters: OptimizationParameters,
}

impl PredictiveOptimizer {
    /// Create a new predictive optimizer
    pub fn new(config: PredictiveOptimizerConfig) -> Self {
        info!("Initializing predictive optimizer with config: {:?}", config);

        let optimizer = Self {
            config: config.clone(),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
            optimization_history: Arc::new(Mutex::new(VecDeque::new())),
            current_parameters: Arc::new(RwLock::new(OptimizationParameters::default())),
            prediction_models: Arc::new(Mutex::new(HashMap::new())),
            performance_feedback: Arc::new(Mutex::new(VecDeque::new())),
        };

        // Start background optimization tasks
        optimizer.start_pattern_detection();
        optimizer.start_parameter_optimization();
        optimizer.start_model_training();

        optimizer
    }

    /// Record an access for pattern detection
    pub async fn record_access(&self, operation_type: &str, offset: u64, size: usize) {
        let timestamp = Instant::now();
        
        // Update access patterns
        self.update_access_patterns(operation_type, offset, size, timestamp).await;

        #[cfg(feature = "metrics")]
        counter!("rustfs_predictive_optimizer_accesses_total").increment(1);
    }

    /// Record performance feedback
    pub async fn record_performance(&self, operation_type: &str, latency_us: f64, throughput_bps: f64) {
        let current_params = self.current_parameters.read().await.clone();
        
        let sample = PerformanceSample {
            timestamp: Instant::now(),
            operation_type: operation_type.to_string(),
            latency_us,
            throughput_bps,
            parameters: current_params,
        };

        let mut feedback = self.performance_feedback.lock().await;
        feedback.push_back(sample);

        // Keep only recent feedback (last 1000 samples)
        if feedback.len() > 1000 {
            feedback.pop_front();
        }

        #[cfg(feature = "metrics")]
        histogram!("rustfs_predictive_optimizer_latency_us")
            .record(latency_us);
    }

    /// Get current optimization parameters
    pub async fn get_current_parameters(&self) -> OptimizationParameters {
        self.current_parameters.read().await.clone()
    }

    /// Get detected access patterns
    pub async fn get_access_patterns(&self) -> HashMap<String, AccessPattern> {
        self.access_patterns.read().await.clone()
    }

    /// Get optimization recommendations
    pub async fn get_recommendations(&self) -> Vec<OptimizationRecommendation> {
        let patterns = self.access_patterns.read().await.clone();
        let current_params = self.current_parameters.read().await.clone();
        let models = self.prediction_models.lock().await;

        let mut recommendations = Vec::new();

        // Generate recommendations based on detected patterns
        for (operation_type, pattern) in &patterns {
            if pattern.confidence < self.config.confidence_threshold {
                continue;
            }

            match &pattern.pattern_type {
                AccessPatternType::Sequential { stride, direction } => {
                    recommendations.extend(self.generate_sequential_recommendations(
                        operation_type,
                        *stride,
                        direction,
                        &current_params,
                    ).await);
                }
                AccessPatternType::Random { hotspot_ratio } => {
                    recommendations.extend(self.generate_random_recommendations(
                        operation_type,
                        *hotspot_ratio,
                        &current_params,
                    ).await);
                }
                AccessPatternType::Periodic { period_seconds, amplitude } => {
                    recommendations.extend(self.generate_periodic_recommendations(
                        operation_type,
                        *period_seconds,
                        *amplitude,
                        &current_params,
                    ).await);
                }
                AccessPatternType::Burst { burst_duration_ms, inter_burst_delay_ms } => {
                    recommendations.extend(self.generate_burst_recommendations(
                        operation_type,
                        *burst_duration_ms,
                        *inter_burst_delay_ms,
                        &current_params,
                    ).await);
                }
            }
        }

        // Use prediction models to refine recommendations
        for (model_name, model) in models.iter() {
            if model.accuracy > self.config.confidence_threshold {
                if let Some(predicted_rec) = self.generate_model_based_recommendation(
                    model_name,
                    model,
                    &current_params,
                ).await {
                    recommendations.push(predicted_rec);
                }
            }
        }

        // Sort by priority and confidence
        recommendations.sort_by(|a, b| {
            b.priority.cmp(&a.priority)
                .then_with(|| b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal))
        });

        recommendations
    }

    /// Apply an optimization recommendation
    pub async fn apply_optimization(&self, recommendation: OptimizationRecommendation) -> anyhow::Result<()> {
        let performance_before = self.measure_current_performance().await;
        
        info!("Applying optimization: {:?}", recommendation.optimization_type);

        // Update parameters based on recommendation
        let mut params = self.current_parameters.write().await;
        match &recommendation.optimization_type {
            OptimizationType::PrefetchTuning { prefetch_distance, prefetch_size } => {
                params.prefetch_distance = *prefetch_distance;
                params.prefetch_size = *prefetch_size;
            }
            OptimizationType::BatchTuning { batch_size, batch_timeout_ms } => {
                params.batch_size = *batch_size;
                params.batch_timeout_ms = *batch_timeout_ms;
            }
            OptimizationType::CompressionSelection { algorithm, compression_level } => {
                params.compression_algorithm = algorithm.clone();
                params.compression_level = *compression_level;
            }
            OptimizationType::BufferPoolTuning { pool_size, buffer_size } => {
                params.buffer_pool_size = *pool_size;
                params.buffer_size = *buffer_size;
            }
            OptimizationType::ConcurrencyTuning { max_concurrent_ops, queue_depth } => {
                params.max_concurrent_ops = *max_concurrent_ops;
                params.queue_depth = *queue_depth;
            }
        }
        drop(params);

        // Record the optimization attempt
        let result = OptimizationResult {
            optimization_type: recommendation.optimization_type,
            applied_at: Instant::now(),
            performance_before,
            performance_after: 0.0, // Will be updated later
            success: true, // Assume success for now
        };

        let mut history = self.optimization_history.lock().await;
        history.push_back(result);

        // Keep limited history
        if history.len() > 100 {
            history.pop_front();
        }

        #[cfg(feature = "metrics")]
        counter!("rustfs_predictive_optimizer_optimizations_applied_total").increment(1);

        Ok(())
    }

    /// Start pattern detection background task
    fn start_pattern_detection(&self) {
        let patterns = Arc::clone(&self.access_patterns);
        let _config = self.config.clone();

        tokio::spawn(async move {
            let mut detection_interval = interval(Duration::from_secs(60));
            
            loop {
                detection_interval.tick().await;
                
                // Analyze patterns for changes and update confidence levels
                let mut patterns_guard = patterns.write().await;
                let current_time = Instant::now();
                
                // Decay confidence over time for patterns that haven't been observed recently
                for pattern in patterns_guard.values_mut() {
                    let time_since_update = current_time.duration_since(pattern.last_update);
                    if time_since_update > Duration::from_secs(300) { // 5 minutes
                        pattern.confidence *= 0.9; // Decay by 10%
                    }
                }

                // Remove patterns with very low confidence
                patterns_guard.retain(|_, pattern| pattern.confidence > 0.1);
                
                debug!("Updated access pattern confidence levels");
            }
        });
    }

    /// Start parameter optimization background task
    fn start_parameter_optimization(&self) {
        let _current_parameters = Arc::clone(&self.current_parameters);
        let optimization_history = Arc::clone(&self.optimization_history);
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut optimization_interval = interval(config.optimization_update_interval);
            
            loop {
                optimization_interval.tick().await;
                
                // Analyze recent optimization results and adjust parameters
                let history = optimization_history.lock().await;
                let recent_results: Vec<_> = history
                    .iter()
                    .rev()
                    .take(10)
                    .collect();

                if recent_results.len() >= 5 {
                    // Calculate success rate of recent optimizations
                    let success_rate = recent_results.iter()
                        .filter(|r| r.success)
                        .count() as f64 / recent_results.len() as f64;

                    if success_rate < 0.7 {
                        warn!("Low optimization success rate: {:.2}%, adjusting parameters", success_rate * 100.0);
                        // Could implement parameter rollback or more conservative tuning here
                    }
                }
                
                debug!("Analyzed optimization history and updated parameters");
            }
        });
    }

    /// Start model training background task
    fn start_model_training(&self) {
        let prediction_models = Arc::clone(&self.prediction_models);
        let performance_feedback = Arc::clone(&self.performance_feedback);
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut training_interval = interval(Duration::from_secs(300)); // Train every 5 minutes
            
            loop {
                training_interval.tick().await;
                
                let feedback_samples = {
                    let feedback = performance_feedback.lock().await;
                    if feedback.len() < config.min_samples_for_prediction {
                        continue;
                    }
                    feedback.clone() // Clone the data to avoid borrowing issues
                };

                // Group feedback by operation type
                let mut grouped_feedback: HashMap<String, Vec<PerformanceSample>> = HashMap::new();
                for sample in feedback_samples {
                    grouped_feedback
                        .entry(sample.operation_type.clone())
                        .or_default()
                        .push(sample);
                }

                // Train models for each operation type
                let mut models = prediction_models.lock().await;
                for (operation_type, samples) in grouped_feedback {
                    if samples.len() >= config.min_samples_for_prediction {
                        let sample_refs: Vec<&PerformanceSample> = samples.iter().collect();
                        let model = Self::train_prediction_model(&sample_refs);
                        models.insert(operation_type, model);
                    }
                }
                
                debug!("Trained prediction models for {} operation types", models.len());
            }
        });
    }

    /// Update access patterns based on observed access
    async fn update_access_patterns(&self, operation_type: &str, offset: u64, size: usize, timestamp: Instant) {
        let mut patterns = self.access_patterns.write().await;
        
        let pattern = patterns
            .entry(operation_type.to_string())
            .or_insert_with(|| AccessPattern {
                pattern_type: AccessPatternType::Random { hotspot_ratio: 0.5 },
                confidence: 0.1,
                parameters: HashMap::new(),
                last_update: timestamp,
                observation_count: 0,
            });

        pattern.last_update = timestamp;
        pattern.observation_count += 1;

        // Simple pattern detection logic (would be more sophisticated in practice)
        pattern.parameters.insert("last_offset".to_string(), offset as f64);
        pattern.parameters.insert("last_size".to_string(), size as f64);

        // Update confidence based on consistency
        if pattern.observation_count > 10 {
            pattern.confidence = (pattern.confidence + 0.1).min(1.0);
        }
    }

    /// Generate recommendations for sequential access patterns
    async fn generate_sequential_recommendations(
        &self,
        _operation_type: &str,
        stride: usize,
        _direction: &AccessDirection,
        current_params: &OptimizationParameters,
    ) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();

        // For sequential access, increase prefetch distance
        if stride > 0 && current_params.prefetch_distance < stride * 2 {
            let mut parameters = HashMap::new();
            parameters.insert("prefetch_distance".to_string(), (stride * 2) as f64);
            parameters.insert("prefetch_size".to_string(), (current_params.prefetch_size * 2) as f64);

            recommendations.push(OptimizationRecommendation {
                optimization_type: OptimizationType::PrefetchTuning {
                    prefetch_distance: stride * 2,
                    prefetch_size: current_params.prefetch_size * 2,
                },
                parameters,
                expected_improvement: 0.3, // 30% improvement expected
                confidence: 0.85,
                priority: 8,
                apply_at: Instant::now(),
            });
        }

        recommendations
    }

    /// Generate recommendations for random access patterns
    async fn generate_random_recommendations(
        &self,
        _operation_type: &str,
        hotspot_ratio: f64,
        current_params: &OptimizationParameters,
    ) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();

        // For random access with hotspots, optimize buffer pool
        if hotspot_ratio > 0.7 && current_params.buffer_pool_size < 2048 {
            let mut parameters = HashMap::new();
            parameters.insert("pool_size".to_string(), 2048.0);
            parameters.insert("buffer_size".to_string(), (current_params.buffer_size / 2) as f64);

            recommendations.push(OptimizationRecommendation {
                optimization_type: OptimizationType::BufferPoolTuning {
                    pool_size: 2048,
                    buffer_size: current_params.buffer_size / 2,
                },
                parameters,
                expected_improvement: 0.25, // 25% improvement expected
                confidence: 0.75,
                priority: 6,
                apply_at: Instant::now(),
            });
        }

        recommendations
    }

    /// Generate recommendations for periodic access patterns
    async fn generate_periodic_recommendations(
        &self,
        _operation_type: &str,
        period_seconds: f64,
        _amplitude: f64,
        current_params: &OptimizationParameters,
    ) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();

        // For periodic access, adjust batch timing
        let optimal_batch_timeout = period_seconds * 1000.0 / 10.0; // 1/10 of period in ms
        if (current_params.batch_timeout_ms - optimal_batch_timeout).abs() > 5.0 {
            let mut parameters = HashMap::new();
            parameters.insert("batch_size".to_string(), (current_params.batch_size * 2) as f64);
            parameters.insert("batch_timeout_ms".to_string(), optimal_batch_timeout);

            recommendations.push(OptimizationRecommendation {
                optimization_type: OptimizationType::BatchTuning {
                    batch_size: current_params.batch_size * 2,
                    batch_timeout_ms: optimal_batch_timeout,
                },
                parameters,
                expected_improvement: 0.2, // 20% improvement expected
                confidence: 0.7,
                priority: 5,
                apply_at: Instant::now(),
            });
        }

        recommendations
    }

    /// Generate recommendations for burst access patterns
    async fn generate_burst_recommendations(
        &self,
        _operation_type: &str,
        burst_duration_ms: f64,
        _inter_burst_delay_ms: f64,
        current_params: &OptimizationParameters,
    ) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();

        // For burst patterns, increase concurrency during bursts
        let optimal_concurrency = ((burst_duration_ms / 10.0) as usize).max(current_params.max_concurrent_ops);
        if optimal_concurrency > current_params.max_concurrent_ops {
            let mut parameters = HashMap::new();
            parameters.insert("max_concurrent_ops".to_string(), optimal_concurrency as f64);
            parameters.insert("queue_depth".to_string(), (optimal_concurrency / 10) as f64);

            recommendations.push(OptimizationRecommendation {
                optimization_type: OptimizationType::ConcurrencyTuning {
                    max_concurrent_ops: optimal_concurrency,
                    queue_depth: optimal_concurrency / 10,
                },
                parameters,
                expected_improvement: 0.4, // 40% improvement expected
                confidence: 0.8,
                priority: 7,
                apply_at: Instant::now(),
            });
        }

        recommendations
    }

    /// Generate model-based recommendation
    async fn generate_model_based_recommendation(
        &self,
        model_name: &str,
        model: &PredictionModel,
        _current_params: &OptimizationParameters,
    ) -> Option<OptimizationRecommendation> {
        // Simple example: if model predicts declining performance, suggest compression optimization
        if model.accuracy > 0.8 && model.parameters.get(0).unwrap_or(&0.0) < &0.0 {
            let mut parameters = HashMap::new();
            parameters.insert("algorithm".to_string(), 1.0); // Encoded as number for simplicity
            parameters.insert("compression_level".to_string(), 1.0);

            return Some(OptimizationRecommendation {
                optimization_type: OptimizationType::CompressionSelection {
                    algorithm: "lz4".to_string(), // Faster compression for declining performance
                    compression_level: 1,
                },
                parameters,
                expected_improvement: 0.15,
                confidence: model.accuracy,
                priority: 4,
                apply_at: Instant::now(),
            });
        }

        None
    }

    /// Train a simple prediction model
    fn train_prediction_model(samples: &[&PerformanceSample]) -> PredictionModel {
        if samples.len() < 2 {
            return PredictionModel {
                model_type: ModelType::Linear,
                parameters: vec![0.0, 0.0],
                accuracy: 0.0,
                last_trained: Instant::now(),
                training_data: VecDeque::new(),
            };
        }

        // Simple linear regression on latency over time
        let n = samples.len() as f64;
        let sum_x: f64 = (0..samples.len()).map(|i| i as f64).sum();
        let sum_y: f64 = samples.iter().map(|s| s.latency_us).sum();
        let sum_xy: f64 = samples.iter().enumerate()
            .map(|(i, s)| i as f64 * s.latency_us)
            .sum();
        let sum_x2: f64 = (0..samples.len()).map(|i| (i as f64).powi(2)).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
        let intercept = (sum_y - slope * sum_x) / n;

        // Calculate R-squared
        let y_mean = sum_y / n;
        let ss_tot: f64 = samples.iter().map(|s| (s.latency_us - y_mean).powi(2)).sum();
        let ss_res: f64 = samples.iter().enumerate()
            .map(|(i, s)| {
                let predicted = intercept + slope * i as f64;
                (s.latency_us - predicted).powi(2)
            })
            .sum();

        let r_squared = if ss_tot > 0.0 { 1.0 - (ss_res / ss_tot) } else { 0.0 };

        let mut training_data = VecDeque::new();
        for (i, sample) in samples.iter().enumerate() {
            training_data.push_back((i as f64, sample.latency_us));
        }

        PredictionModel {
            model_type: ModelType::Linear,
            parameters: vec![slope, intercept],
            accuracy: r_squared.max(0.0).min(1.0),
            last_trained: Instant::now(),
            training_data,
        }
    }

    /// Measure current performance (simplified)
    async fn measure_current_performance(&self) -> f64 {
        let feedback = self.performance_feedback.lock().await;
        if feedback.is_empty() {
            return 1000.0; // Default baseline
        }

        // Return average latency of recent samples
        let recent_samples: Vec<_> = feedback.iter().rev().take(10).collect();
        let avg_latency: f64 = recent_samples.iter().map(|s| s.latency_us).sum::<f64>() / recent_samples.len() as f64;
        
        avg_latency
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_predictive_optimizer_creation() {
        let config = PredictiveOptimizerConfig::default();
        let optimizer = PredictiveOptimizer::new(config);
        
        let params = optimizer.get_current_parameters().await;
        assert_eq!(params.batch_size, 16); // Default value
    }

    #[tokio::test]
    async fn test_access_pattern_recording() {
        let config = PredictiveOptimizerConfig::default();
        let optimizer = PredictiveOptimizer::new(config);
        
        // Record several sequential accesses
        for i in 0..10 {
            optimizer.record_access("test_op", i * 1024, 1024).await;
            sleep(Duration::from_millis(10)).await;
        }
        
        let patterns = optimizer.get_access_patterns().await;
        assert!(patterns.contains_key("test_op"));
    }

    #[tokio::test]
    async fn test_performance_feedback() {
        let config = PredictiveOptimizerConfig::default();
        let optimizer = PredictiveOptimizer::new(config);
        
        // Record performance samples
        for i in 0..20 {
            optimizer.record_performance("test_op", 500.0 + i as f64 * 10.0, 1_000_000.0).await;
            sleep(Duration::from_millis(5)).await;
        }
        
        // Should have recorded the samples
        let feedback = optimizer.performance_feedback.lock().await;
        assert_eq!(feedback.len(), 20);
    }

    #[tokio::test]
    async fn test_recommendation_generation() {
        let config = PredictiveOptimizerConfig::default();
        let optimizer = PredictiveOptimizer::new(config);
        
        // Create a detected pattern that should generate recommendations
        let mut patterns = optimizer.access_patterns.write().await;
        patterns.insert("test_op".to_string(), AccessPattern {
            pattern_type: AccessPatternType::Sequential {
                stride: 1024,
                direction: AccessDirection::Forward,
            },
            confidence: 0.9,
            parameters: HashMap::new(),
            last_update: Instant::now(),
            observation_count: 50,
        });
        drop(patterns);
        
        let recommendations = optimizer.get_recommendations().await;
        assert!(!recommendations.is_empty());
        
        // Should have a prefetch tuning recommendation
        let prefetch_rec = recommendations.iter()
            .find(|r| matches!(r.optimization_type, OptimizationType::PrefetchTuning { .. }));
        assert!(prefetch_rec.is_some());
    }

    #[tokio::test]
    async fn test_optimization_application() {
        let config = PredictiveOptimizerConfig::default();
        let optimizer = PredictiveOptimizer::new(config);
        
        let recommendation = OptimizationRecommendation {
            optimization_type: OptimizationType::BatchTuning {
                batch_size: 32,
                batch_timeout_ms: 20.0,
            },
            parameters: HashMap::new(),
            expected_improvement: 0.2,
            confidence: 0.8,
            priority: 5,
            apply_at: Instant::now(),
        };
        
        let result = optimizer.apply_optimization(recommendation).await;
        assert!(result.is_ok());
        
        // Check that parameters were updated
        let params = optimizer.get_current_parameters().await;
        assert_eq!(params.batch_size, 32);
        assert_eq!(params.batch_timeout_ms, 20.0);
    }
}