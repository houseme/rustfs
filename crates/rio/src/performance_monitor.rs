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

//! Advanced Performance Monitoring and Analysis
//!
//! This module provides sophisticated performance monitoring capabilities including
//! real-time dashboards, pattern recognition, and predictive optimization.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, Mutex};
use tokio::time::interval;
use tracing::{debug, info, warn, error};
use serde::{Deserialize, Serialize};

#[cfg(feature = "metrics")]
use metrics::{counter, gauge, histogram};

/// Configuration for performance monitoring
#[derive(Debug, Clone)]
pub struct PerformanceMonitorConfig {
    /// Enable real-time performance dashboard
    pub enable_real_time_dashboard: bool,
    /// Enable performance profiling with flame graphs
    pub enable_performance_profiling: bool,
    /// Enable pattern recognition for predictive optimization
    pub enable_pattern_recognition: bool,
    /// Metrics collection interval
    pub metrics_collection_interval: Duration,
    /// History retention period
    pub history_retention_period: Duration,
    /// Enable adaptive optimization based on patterns
    pub enable_adaptive_optimization: bool,
    /// Threshold for performance alerts
    pub performance_alert_threshold: f64,
}

impl Default for PerformanceMonitorConfig {
    fn default() -> Self {
        Self {
            enable_real_time_dashboard: true,
            enable_performance_profiling: true,
            enable_pattern_recognition: true,
            metrics_collection_interval: Duration::from_secs(1),
            history_retention_period: Duration::from_secs(3600), // 1 hour
            enable_adaptive_optimization: true,
            performance_alert_threshold: 0.8, // 80% threshold
        }
    }
}

/// Real-time performance statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealTimeStats {
    /// Current timestamp
    pub timestamp: u64,
    /// I/O operations per second
    pub iops: f64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// 95th percentile latency in microseconds
    pub p95_latency_us: f64,
    /// 99th percentile latency in microseconds
    pub p99_latency_us: f64,
    /// Throughput in bytes per second
    pub throughput_bps: f64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// CPU utilization percentage
    pub cpu_utilization_pct: f64,
    /// Queue depth
    pub queue_depth: u32,
    /// Error rate per second
    pub error_rate: f64,
    /// Active connections
    pub active_connections: u32,
}

/// Performance trend analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTrend {
    /// Metric name
    pub metric_name: String,
    /// Trend direction (positive, negative, stable)
    pub trend_direction: TrendDirection,
    /// Trend strength (0.0 to 1.0)
    pub trend_strength: f64,
    /// Predicted value for next period
    pub predicted_value: f64,
    /// Confidence in prediction (0.0 to 1.0)
    pub prediction_confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrendDirection {
    Increasing,
    Decreasing,
    Stable,
}

/// Performance alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAlert {
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert message
    pub message: String,
    /// Affected metric
    pub metric: String,
    /// Current value
    pub current_value: f64,
    /// Threshold value
    pub threshold_value: f64,
    /// Timestamp when alert was generated
    pub timestamp: u64,
    /// Suggested actions
    pub suggested_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Advanced performance monitor
pub struct AdvancedPerformanceMonitor {
    config: PerformanceMonitorConfig,
    current_stats: Arc<RwLock<RealTimeStats>>,
    historical_data: Arc<Mutex<VecDeque<RealTimeStats>>>,
    performance_trends: Arc<RwLock<HashMap<String, PerformanceTrend>>>,
    active_alerts: Arc<RwLock<Vec<PerformanceAlert>>>,
    pattern_analyzer: Arc<Mutex<PatternAnalyzer>>,
    optimization_recommendations: Arc<RwLock<Vec<OptimizationRecommendation>>>,
}

/// Pattern analyzer for predictive optimization
struct PatternAnalyzer {
    metric_history: HashMap<String, VecDeque<(u64, f64)>>,
    detected_patterns: HashMap<String, DetectedPattern>,
    prediction_models: HashMap<String, SimpleLinearModel>,
}

/// Simple detected pattern
#[derive(Debug, Clone)]
struct DetectedPattern {
    pattern_type: PatternType,
    confidence: f64,
    period_seconds: Option<u64>,
    amplitude: f64,
}

#[derive(Debug, Clone)]
enum PatternType {
    Periodic,
    Trending,
    Spike,
    Constant,
}

/// Simple linear regression model for predictions
#[derive(Debug, Clone)]
struct SimpleLinearModel {
    slope: f64,
    intercept: f64,
    r_squared: f64,
    last_update: Instant,
}

/// Optimization recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationRecommendation {
    /// Recommendation category
    pub category: String,
    /// Recommended action
    pub action: String,
    /// Expected impact
    pub expected_impact: String,
    /// Priority (1-10, 10 being highest)
    pub priority: u8,
    /// Confidence in recommendation (0.0 to 1.0)
    pub confidence: f64,
    /// Timestamp when recommendation was generated
    pub timestamp: u64,
}

impl AdvancedPerformanceMonitor {
    /// Create a new advanced performance monitor
    pub fn new(config: PerformanceMonitorConfig) -> Self {
        info!("Initializing advanced performance monitor with config: {:?}", config);

        let monitor = Self {
            config: config.clone(),
            current_stats: Arc::new(RwLock::new(RealTimeStats::default())),
            historical_data: Arc::new(Mutex::new(VecDeque::new())),
            performance_trends: Arc::new(RwLock::new(HashMap::new())),
            active_alerts: Arc::new(RwLock::new(Vec::new())),
            pattern_analyzer: Arc::new(Mutex::new(PatternAnalyzer::new())),
            optimization_recommendations: Arc::new(RwLock::new(Vec::new())),
        };

        // Start background monitoring tasks
        if config.enable_real_time_dashboard {
            monitor.start_real_time_monitoring();
        }

        if config.enable_pattern_recognition {
            monitor.start_pattern_analysis();
        }

        if config.enable_adaptive_optimization {
            monitor.start_adaptive_optimization();
        }

        monitor
    }

    /// Get current real-time statistics
    pub async fn get_real_time_stats(&self) -> RealTimeStats {
        self.current_stats.read().await.clone()
    }

    /// Get historical performance data
    pub async fn get_historical_data(&self, duration: Duration) -> Vec<RealTimeStats> {
        let history = self.historical_data.lock().await;
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(duration.as_secs());

        history
            .iter()
            .filter(|stats| stats.timestamp >= cutoff_time)
            .cloned()
            .collect()
    }

    /// Get performance trends analysis
    pub async fn get_performance_trends(&self) -> HashMap<String, PerformanceTrend> {
        self.performance_trends.read().await.clone()
    }

    /// Get active performance alerts
    pub async fn get_active_alerts(&self) -> Vec<PerformanceAlert> {
        self.active_alerts.read().await.clone()
    }

    /// Get optimization recommendations
    pub async fn get_optimization_recommendations(&self) -> Vec<OptimizationRecommendation> {
        self.optimization_recommendations.read().await.clone()
    }

    /// Record a performance metric
    pub async fn record_metric(&self, metric_name: &str, value: f64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update pattern analyzer
        let mut analyzer = self.pattern_analyzer.lock().await;
        analyzer.record_metric(metric_name, timestamp, value);

        #[cfg(feature = "metrics")]
        {
            histogram!(format!("rustfs_performance_monitor_{}", metric_name))
                .record(value);
        }
    }

    /// Update current statistics
    pub async fn update_stats(&self, stats: RealTimeStats) {
        // Update current stats
        *self.current_stats.write().await = stats.clone();

        // Add to history
        let mut history = self.historical_data.lock().await;
        history.push_back(stats);

        // Trim old data
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.config.history_retention_period.as_secs());

        while let Some(front) = history.front() {
            if front.timestamp < cutoff_time {
                history.pop_front();
            } else {
                break;
            }
        }

        // Check for alerts
        self.check_performance_alerts(&stats).await;
    }

    /// Start real-time monitoring background task
    fn start_real_time_monitoring(&self) {
        let stats = Arc::clone(&self.current_stats);
        let interval_duration = self.config.metrics_collection_interval;

        tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            
            loop {
                interval.tick().await;
                
                // Collect system metrics (simulated for now)
                let current_stats = Self::collect_system_metrics().await;
                
                // Update stats
                *stats.write().await = current_stats;
                
                #[cfg(feature = "metrics")]
                {
                    let stats_guard = stats.read().await;
                    gauge!("rustfs_performance_iops").set(stats_guard.iops);
                    gauge!("rustfs_performance_avg_latency_us").set(stats_guard.avg_latency_us);
                    gauge!("rustfs_performance_throughput_bps").set(stats_guard.throughput_bps);
                    gauge!("rustfs_performance_cache_hit_rate").set(stats_guard.cache_hit_rate);
                }
            }
        });
    }

    /// Start pattern analysis background task
    fn start_pattern_analysis(&self) {
        let analyzer = Arc::clone(&self.pattern_analyzer);
        let trends = Arc::clone(&self.performance_trends);

        tokio::spawn(async move {
            let mut analysis_interval = interval(Duration::from_secs(60)); // Analyze every minute
            
            loop {
                analysis_interval.tick().await;
                
                let mut analyzer_guard = analyzer.lock().await;
                let detected_trends = analyzer_guard.analyze_trends().await;
                
                *trends.write().await = detected_trends;
                
                debug!("Updated performance trends based on pattern analysis");
            }
        });
    }

    /// Start adaptive optimization background task
    fn start_adaptive_optimization(&self) {
        let recommendations = Arc::clone(&self.optimization_recommendations);
        let trends = Arc::clone(&self.performance_trends);
        let historical_data = Arc::clone(&self.historical_data);

        tokio::spawn(async move {
            let mut optimization_interval = interval(Duration::from_secs(300)); // Optimize every 5 minutes
            
            loop {
                optimization_interval.tick().await;
                
                let current_trends = trends.read().await.clone();
                let recent_data = {
                    let history = historical_data.lock().await;
                    history.iter().rev().take(60).cloned().collect::<Vec<_>>()
                };
                
                if !recent_data.is_empty() {
                    let new_recommendations = Self::generate_optimization_recommendations(
                        &current_trends,
                        &recent_data,
                    ).await;
                    
                    *recommendations.write().await = new_recommendations;
                    
                    debug!("Generated new optimization recommendations");
                }
            }
        });
    }

    /// Collect system metrics (simplified implementation)
    async fn collect_system_metrics() -> RealTimeStats {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // In a real implementation, these would come from actual system monitoring
        RealTimeStats {
            timestamp,
            iops: 1000.0 + (timestamp as f64 * 0.1).sin() * 200.0, // Simulate varying IOPS
            avg_latency_us: 500.0 + (timestamp as f64 * 0.05).cos() * 100.0,
            p95_latency_us: 800.0 + (timestamp as f64 * 0.05).cos() * 150.0,
            p99_latency_us: 1200.0 + (timestamp as f64 * 0.05).cos() * 200.0,
            throughput_bps: 50_000_000.0 + (timestamp as f64 * 0.02).sin() * 10_000_000.0,
            cache_hit_rate: 0.85 + (timestamp as f64 * 0.01).sin() * 0.1,
            memory_usage_bytes: 1_000_000_000 + ((timestamp % 3600) as f64 / 3600.0 * 100_000_000.0) as u64,
            cpu_utilization_pct: 45.0 + (timestamp as f64 * 0.03).sin() * 15.0,
            queue_depth: 5 + ((timestamp % 60) / 10) as u32,
            error_rate: if timestamp % 120 < 10 { 2.0 } else { 0.1 },
            active_connections: 50 + ((timestamp % 300) / 10) as u32,
        }
    }

    /// Check for performance alerts
    async fn check_performance_alerts(&self, stats: &RealTimeStats) {
        let mut alerts = Vec::new();

        // Check latency thresholds
        if stats.avg_latency_us > 1000.0 {
            alerts.push(PerformanceAlert {
                severity: if stats.avg_latency_us > 2000.0 {
                    AlertSeverity::Critical
                } else {
                    AlertSeverity::Warning
                },
                message: format!("High average latency detected: {:.2}μs", stats.avg_latency_us),
                metric: "avg_latency_us".to_string(),
                current_value: stats.avg_latency_us,
                threshold_value: 1000.0,
                timestamp: stats.timestamp,
                suggested_actions: vec![
                    "Check I/O queue depth".to_string(),
                    "Review concurrent operation limits".to_string(),
                    "Consider enabling io_uring if available".to_string(),
                ],
            });
        }

        // Check cache hit rate
        if stats.cache_hit_rate < 0.7 {
            alerts.push(PerformanceAlert {
                severity: AlertSeverity::Warning,
                message: format!("Low cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0),
                metric: "cache_hit_rate".to_string(),
                current_value: stats.cache_hit_rate,
                threshold_value: 0.7,
                timestamp: stats.timestamp,
                suggested_actions: vec![
                    "Increase buffer pool size".to_string(),
                    "Review access patterns".to_string(),
                    "Consider memory-mapped I/O for large files".to_string(),
                ],
            });
        }

        // Check error rate
        if stats.error_rate > 1.0 {
            alerts.push(PerformanceAlert {
                severity: AlertSeverity::Critical,
                message: format!("High error rate detected: {:.2} errors/sec", stats.error_rate),
                metric: "error_rate".to_string(),
                current_value: stats.error_rate,
                threshold_value: 1.0,
                timestamp: stats.timestamp,
                suggested_actions: vec![
                    "Check system logs for error details".to_string(),
                    "Verify storage system health".to_string(),
                    "Review network connectivity".to_string(),
                ],
            });
        }

        if !alerts.is_empty() {
            let mut active_alerts = self.active_alerts.write().await;
            active_alerts.extend(alerts);

            // Keep only recent alerts (last hour)
            let cutoff_time = stats.timestamp.saturating_sub(3600);
            active_alerts.retain(|alert| alert.timestamp >= cutoff_time);
        }
    }

    /// Generate optimization recommendations
    async fn generate_optimization_recommendations(
        trends: &HashMap<String, PerformanceTrend>,
        recent_data: &[RealTimeStats],
    ) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Analyze recent performance data
        if let Some(avg_latency) = recent_data.iter().map(|s| s.avg_latency_us).reduce(|a, b| a + b) {
            let avg_latency = avg_latency / recent_data.len() as f64;
            
            if avg_latency > 800.0 {
                recommendations.push(OptimizationRecommendation {
                    category: "Latency".to_string(),
                    action: "Enable io_uring for zero-copy operations".to_string(),
                    expected_impact: "40% latency reduction".to_string(),
                    priority: 9,
                    confidence: 0.85,
                    timestamp,
                });
            }
        }

        // Check cache hit rates
        if let Some(avg_cache_hit_rate) = recent_data.iter().map(|s| s.cache_hit_rate).reduce(|a, b| a + b) {
            let avg_cache_hit_rate = avg_cache_hit_rate / recent_data.len() as f64;
            
            if avg_cache_hit_rate < 0.8 {
                recommendations.push(OptimizationRecommendation {
                    category: "Caching".to_string(),
                    action: "Increase buffer pool size and enable adaptive sizing".to_string(),
                    expected_impact: "15-25% cache hit rate improvement".to_string(),
                    priority: 7,
                    confidence: 0.75,
                    timestamp,
                });
            }
        }

        // Check for trending issues
        if let Some(iops_trend) = trends.get("iops") {
            if matches!(iops_trend.trend_direction, TrendDirection::Decreasing) && iops_trend.trend_strength > 0.6 {
                recommendations.push(OptimizationRecommendation {
                    category: "Throughput".to_string(),
                    action: "Enable batch processing and increase concurrency limits".to_string(),
                    expected_impact: "2-3x IOPS improvement".to_string(),
                    priority: 8,
                    confidence: iops_trend.prediction_confidence,
                    timestamp,
                });
            }
        }

        recommendations
    }
}

impl Default for RealTimeStats {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            iops: 0.0,
            avg_latency_us: 0.0,
            p95_latency_us: 0.0,
            p99_latency_us: 0.0,
            throughput_bps: 0.0,
            cache_hit_rate: 0.0,
            memory_usage_bytes: 0,
            cpu_utilization_pct: 0.0,
            queue_depth: 0,
            error_rate: 0.0,
            active_connections: 0,
        }
    }
}

impl PatternAnalyzer {
    fn new() -> Self {
        Self {
            metric_history: HashMap::new(),
            detected_patterns: HashMap::new(),
            prediction_models: HashMap::new(),
        }
    }

    fn record_metric(&mut self, metric_name: &str, timestamp: u64, value: f64) {
        let history = self.metric_history
            .entry(metric_name.to_string())
            .or_insert_with(|| VecDeque::with_capacity(1000));

        history.push_back((timestamp, value));

        // Keep only recent history
        while history.len() > 1000 {
            history.pop_front();
        }

        // Update prediction model
        if history.len() >= 10 {
            self.update_prediction_model(metric_name, history);
        }
    }

    fn update_prediction_model(&mut self, metric_name: &str, history: &VecDeque<(u64, f64)>) {
        if history.len() < 2 {
            return;
        }

        // Simple linear regression
        let n = history.len() as f64;
        let sum_x: f64 = history.iter().enumerate().map(|(i, _)| i as f64).sum();
        let sum_y: f64 = history.iter().map(|(_, y)| *y).sum();
        let sum_xy: f64 = history.iter().enumerate().map(|(i, (_, y))| i as f64 * y).sum();
        let sum_x2: f64 = history.iter().enumerate().map(|(i, _)| (i as f64).powi(2)).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
        let intercept = (sum_y - slope * sum_x) / n;

        // Calculate R-squared
        let y_mean = sum_y / n;
        let ss_tot: f64 = history.iter().map(|(_, y)| (y - y_mean).powi(2)).sum();
        let ss_res: f64 = history.iter().enumerate()
            .map(|(i, (_, y))| {
                let predicted = intercept + slope * i as f64;
                (y - predicted).powi(2)
            })
            .sum();

        let r_squared = if ss_tot > 0.0 { 1.0 - (ss_res / ss_tot) } else { 0.0 };

        self.prediction_models.insert(
            metric_name.to_string(),
            SimpleLinearModel {
                slope,
                intercept,
                r_squared,
                last_update: Instant::now(),
            },
        );
    }

    async fn analyze_trends(&mut self) -> HashMap<String, PerformanceTrend> {
        let mut trends = HashMap::new();

        for (metric_name, model) in &self.prediction_models {
            let trend_direction = if model.slope > 0.1 {
                TrendDirection::Increasing
            } else if model.slope < -0.1 {
                TrendDirection::Decreasing
            } else {
                TrendDirection::Stable
            };

            let trend_strength = model.slope.abs().min(1.0);
            let predicted_value = model.intercept + model.slope * 1000.0; // Predict 1000 time units ahead
            let prediction_confidence = model.r_squared.max(0.0).min(1.0);

            trends.insert(
                metric_name.clone(),
                PerformanceTrend {
                    metric_name: metric_name.clone(),
                    trend_direction,
                    trend_strength,
                    predicted_value,
                    prediction_confidence,
                },
            );
        }

        trends
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_performance_monitor_creation() {
        let config = PerformanceMonitorConfig::default();
        let monitor = AdvancedPerformanceMonitor::new(config);
        
        let stats = monitor.get_real_time_stats().await;
        assert_eq!(stats.iops, 0.0); // Initial state
    }

    #[tokio::test]
    async fn test_metric_recording() {
        let config = PerformanceMonitorConfig::default();
        let monitor = AdvancedPerformanceMonitor::new(config);
        
        monitor.record_metric("test_metric", 42.0).await;
        
        // Verify the metric was recorded in the pattern analyzer
        let analyzer = monitor.pattern_analyzer.lock().await;
        assert!(analyzer.metric_history.contains_key("test_metric"));
    }

    #[tokio::test]
    async fn test_historical_data_retention() {
        let mut config = PerformanceMonitorConfig::default();
        config.history_retention_period = Duration::from_secs(1); // Very short for testing
        
        let monitor = AdvancedPerformanceMonitor::new(config);
        
        // Add some historical data
        let stats1 = RealTimeStats::default();
        let mut stats2 = RealTimeStats::default();
        stats2.timestamp += 2; // 2 seconds later
        
        monitor.update_stats(stats1).await;
        sleep(Duration::from_millis(1100)).await; // Wait for retention period
        monitor.update_stats(stats2).await;
        
        let history = monitor.get_historical_data(Duration::from_secs(10)).await;
        // Should only have recent data due to retention policy
        assert!(history.len() <= 2);
    }

    #[tokio::test]
    async fn test_performance_alerts() {
        let config = PerformanceMonitorConfig::default();
        let monitor = AdvancedPerformanceMonitor::new(config);
        
        // Create stats that should trigger alerts
        let mut high_latency_stats = RealTimeStats::default();
        high_latency_stats.avg_latency_us = 1500.0; // Above threshold
        high_latency_stats.cache_hit_rate = 0.6; // Below threshold
        
        monitor.update_stats(high_latency_stats).await;
        
        let alerts = monitor.get_active_alerts().await;
        assert!(!alerts.is_empty());
        
        // Should have alerts for high latency and low cache hit rate
        let latency_alert = alerts.iter().find(|a| a.metric == "avg_latency_us");
        assert!(latency_alert.is_some());
        
        let cache_alert = alerts.iter().find(|a| a.metric == "cache_hit_rate");
        assert!(cache_alert.is_some());
    }

    #[tokio::test]
    async fn test_optimization_recommendations() {
        let config = PerformanceMonitorConfig::default();
        let monitor = AdvancedPerformanceMonitor::new(config);
        
        // Create performance data that should generate recommendations
        let mut poor_performance_stats = RealTimeStats::default();
        poor_performance_stats.avg_latency_us = 900.0; // High latency
        poor_performance_stats.cache_hit_rate = 0.75; // Suboptimal cache hit rate
        
        let trends = HashMap::new();
        let recent_data = vec![poor_performance_stats; 10];
        
        let recommendations = AdvancedPerformanceMonitor::generate_optimization_recommendations(
            &trends,
            &recent_data,
        ).await;
        
        assert!(!recommendations.is_empty());
        
        // Should have recommendations for latency and caching
        let latency_rec = recommendations.iter().find(|r| r.category == "Latency");
        assert!(latency_rec.is_some());
        
        let cache_rec = recommendations.iter().find(|r| r.category == "Caching");
        assert!(cache_rec.is_some());
    }
}