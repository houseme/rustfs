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

//! Core data types for topology management

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Node health state enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    /// Node is fully operational and responding normally
    Healthy,

    /// Node is operational but experiencing degraded performance
    /// (high latency, elevated error rate)
    Degraded,

    /// Node is not reachable via network but may recover
    Unreachable,

    /// Node is offline (power failure, crash, etc.)
    /// Includes timestamp of when it went offline and failure count
    Offline { since: DateTime<Utc>, failure_count: u32 },

    /// Node has been manually suspended by administrator
    Suspended,

    /// Node state is unknown (initial state before first health check)
    #[default]
    Unknown,
}

impl NodeHealth {
    /// Returns true if the node is considered operational for serving requests
    #[inline]
    pub fn is_operational(&self) -> bool {
        matches!(self, NodeHealth::Healthy | NodeHealth::Degraded)
    }

    /// Returns true if the node is in a failed state
    #[inline]
    pub fn is_failed(&self) -> bool {
        matches!(self, NodeHealth::Unreachable | NodeHealth::Offline { .. } | NodeHealth::Suspended)
    }

    /// Returns a numeric score for health (0-100, higher is better)
    pub fn health_score(&self) -> u8 {
        match self {
            NodeHealth::Healthy => 100,
            NodeHealth::Degraded => 60,
            NodeHealth::Unreachable => 20,
            NodeHealth::Offline { .. } => 0,
            NodeHealth::Suspended => 0,
            NodeHealth::Unknown => 50,
        }
    }
}

/// Performance metrics for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Most recent response latency in milliseconds
    pub latency_ms: Option<u64>,

    /// Error rate (0.0-1.0)
    pub error_rate: f64,

    /// Timestamp of last successful operation
    pub last_success: Option<DateTime<Utc>>,

    /// Timestamp of last failed operation
    pub last_failure: Option<DateTime<Utc>>,

    /// Total number of requests sent to this node
    pub request_count: u64,

    /// Total number of successful requests
    pub success_count: u64,

    /// Moving average of latency over last N requests (ms)
    pub avg_latency_ms: Option<u64>,

    /// 95th percentile latency (ms)
    pub p95_latency_ms: Option<u64>,

    /// 99th percentile latency (ms)
    pub p99_latency_ms: Option<u64>,
}

impl NodeMetrics {
    /// Create a new NodeMetrics instance with default values
    pub fn new() -> Self {
        Self {
            latency_ms: None,
            error_rate: 0.0,
            last_success: None,
            last_failure: None,
            request_count: 0,
            success_count: 0,
            avg_latency_ms: None,
            p95_latency_ms: None,
            p99_latency_ms: None,
        }
    }

    /// Record a successful operation
    pub fn record_success(&mut self, latency_ms: u64) {
        self.request_count += 1;
        self.success_count += 1;
        self.latency_ms = Some(latency_ms);
        self.last_success = Some(Utc::now());
        self.update_error_rate();
    }

    /// Record a failed operation
    pub fn record_failure(&mut self) {
        self.request_count += 1;
        self.last_failure = Some(Utc::now());
        self.update_error_rate();
    }

    /// Update error rate based on recent history
    fn update_error_rate(&mut self) {
        if self.request_count > 0 {
            let failures = self.request_count - self.success_count;
            self.error_rate = failures as f64 / self.request_count as f64;
        }
    }

    /// Check if node is degraded based on metrics
    pub fn is_degraded(&self, degraded_latency_threshold_ms: u64, error_rate_threshold: f64) -> bool {
        if let Some(latency) = self.latency_ms
            && latency > degraded_latency_threshold_ms
        {
            return true;
        }

        self.error_rate > error_rate_threshold
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete status information for a node
#[derive(Debug, Clone, Serialize)]
pub struct NodeStatus {
    /// Node network endpoint (e.g., "node1.example.com:9000")
    pub endpoint: String,

    /// Unique node identifier
    pub node_id: String,

    /// Current health state
    pub health: NodeHealth,

    /// Performance metrics
    pub metrics: NodeMetrics,

    /// Software version running on the node
    pub version: String,

    /// Capabilities supported by this node
    pub capabilities: Vec<String>,

    /// Last health check timestamp
    #[serde(with = "instant_serialization")]
    pub last_check: Instant,

    /// When this node was first discovered
    #[serde(with = "instant_serialization")]
    pub first_seen: Instant,

    /// Custom tags/labels for the node
    pub tags: HashMap<String, String>,

    /// Physical location or zone
    pub zone: Option<String>,

    /// Rack or availability group
    pub rack: Option<String>,
}

impl NodeStatus {
    /// Create a new NodeStatus
    pub fn new(endpoint: String, node_id: String) -> Self {
        let now = Instant::now();
        Self {
            endpoint,
            node_id,
            health: NodeHealth::Unknown,
            metrics: NodeMetrics::new(),
            version: String::new(),
            capabilities: Vec::new(),
            last_check: now,
            first_seen: now,
            tags: HashMap::new(),
            zone: None,
            rack: None,
        }
    }

    /// Update health status
    pub fn set_health(&mut self, health: NodeHealth) {
        self.health = health;
        self.last_check = Instant::now();
    }

    /// Check if node should be evicted based on time offline
    pub fn should_evict(&self, offline_threshold: Duration) -> bool {
        if let NodeHealth::Offline { since, .. } = self.health {
            let offline_duration = Utc::now() - since;
            return offline_duration.to_std().unwrap_or(Duration::ZERO) > offline_threshold;
        }
        false
    }
}

/// Configuration for the topology system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConfig {
    /// Cluster identifier
    pub cluster_id: String,

    /// Health check interval in seconds
    pub health_check_interval_secs: u64,

    /// Timeout for health check requests
    pub health_check_timeout_secs: u64,

    /// Number of consecutive failures before marking node as offline
    pub failure_threshold: u32,

    /// Latency threshold for marking node as degraded (ms)
    pub degraded_latency_threshold_ms: u64,

    /// Error rate threshold for marking node as degraded (0.0-1.0)
    pub degraded_error_rate_threshold: f64,

    /// Duration after which offline nodes are evicted from topology
    pub offline_eviction_timeout_secs: u64,

    /// Enable automatic recovery attempts for offline nodes
    pub enable_auto_recovery: bool,

    /// Interval for attempting recovery of offline nodes (seconds)
    pub recovery_attempt_interval_secs: u64,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            cluster_id: String::from("default-cluster"),
            health_check_interval_secs: 5,
            health_check_timeout_secs: 3,
            failure_threshold: 3,
            degraded_latency_threshold_ms: 1000,
            degraded_error_rate_threshold: 0.1,
            offline_eviction_timeout_secs: 300,
            enable_auto_recovery: true,
            recovery_attempt_interval_secs: 30,
        }
    }
}

/// Cluster-wide statistics
#[derive(Debug, Clone, Serialize)]
pub struct ClusterStats {
    /// Total number of nodes in the topology
    pub total_nodes: usize,

    /// Number of healthy nodes
    pub healthy_nodes: usize,

    /// Number of degraded nodes
    pub degraded_nodes: usize,

    /// Number of unreachable nodes
    pub unreachable_nodes: usize,

    /// Number of offline nodes
    pub offline_nodes: usize,

    /// Number of suspended nodes
    pub suspended_nodes: usize,

    /// Number of nodes with unknown status
    pub unknown_nodes: usize,

    /// Average latency across all healthy nodes (ms)
    pub avg_latency_ms: Option<u64>,

    /// Average error rate across all nodes
    pub avg_error_rate: f64,
}

impl ClusterStats {
    /// Calculate quorum availability (fraction of operational nodes)
    pub fn quorum_availability(&self) -> f64 {
        if self.total_nodes == 0 {
            return 0.0;
        }
        let operational = self.healthy_nodes + self.degraded_nodes;
        operational as f64 / self.total_nodes as f64
    }

    /// Check if cluster has write quorum (> N/2 nodes operational)
    pub fn has_write_quorum(&self) -> bool {
        let operational = self.healthy_nodes + self.degraded_nodes;
        operational > self.total_nodes / 2
    }

    /// Check if cluster has read quorum (>= N/2 nodes operational)
    pub fn has_read_quorum(&self) -> bool {
        let operational = self.healthy_nodes + self.degraded_nodes;
        operational >= self.total_nodes / 2
    }
}

/// Helper module for Instant serialization
mod instant_serialization {
    use serde::Serializer;
    use std::time::Instant;

    pub fn serialize<S>(instant: &Instant, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as elapsed time since creation
        let elapsed = instant.elapsed().as_secs();
        serializer.serialize_u64(elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_health_is_operational() {
        assert!(NodeHealth::Healthy.is_operational());
        assert!(NodeHealth::Degraded.is_operational());
        assert!(!NodeHealth::Unreachable.is_operational());
        assert!(
            !NodeHealth::Offline {
                since: Utc::now(),
                failure_count: 1
            }
            .is_operational()
        );
    }

    #[test]
    fn test_node_metrics_error_rate() {
        let mut metrics = NodeMetrics::new();

        metrics.record_success(100);
        assert_eq!(metrics.error_rate, 0.0);

        metrics.record_failure();
        assert!(metrics.error_rate > 0.0);
        assert_eq!(metrics.request_count, 2);
        assert_eq!(metrics.success_count, 1);
    }

    #[test]
    fn test_cluster_stats_quorum() {
        let stats = ClusterStats {
            total_nodes: 4,
            healthy_nodes: 3,
            degraded_nodes: 0,
            unreachable_nodes: 1,
            offline_nodes: 0,
            suspended_nodes: 0,
            unknown_nodes: 0,
            avg_latency_ms: Some(100),
            avg_error_rate: 0.0,
        };

        assert!(stats.has_write_quorum());
        assert!(stats.has_read_quorum());
        assert_eq!(stats.quorum_availability(), 0.75);
    }
}
