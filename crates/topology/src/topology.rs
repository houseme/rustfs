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

//! SystemTopology implementation - centralized node state management

use crate::types::*;
use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Global topology state manager for RustFS cluster
///
/// Maintains centralized view of all nodes in the cluster with their health
/// states, metrics, and metadata. Provides fast lock-free reads for hot paths.
#[derive(Debug, Clone)]
pub struct SystemTopology {
    /// Cluster identifier
    cluster_id: String,

    /// Configuration
    config: TopologyConfig,

    /// Concurrent map of node_id -> NodeStatus
    /// DashMap allows lock-free concurrent reads
    nodes: Arc<DashMap<String, Arc<RwLock<NodeStatus>>>>,

    /// Cluster-wide statistics (cached, periodically updated)
    stats: Arc<RwLock<ClusterStats>>,
}

impl SystemTopology {
    /// Create new topology instance
    ///
    /// # Arguments
    ///
    /// * `cluster_id` - Unique cluster identifier
    /// * `config` - Topology configuration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rustfs_topology::{SystemTopology, TopologyConfig};
    ///
    /// #[tokio::main]
    /// async fn main() -> anyhow::Result<()> {
    ///     let config = TopologyConfig::default();
    ///     let topology = SystemTopology::new("my-cluster", config).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn new(cluster_id: &str, config: TopologyConfig) -> Result<Self> {
        info!(cluster_id = %cluster_id, "Initializing SystemTopology");

        let topology = Self {
            cluster_id: cluster_id.to_string(),
            config,
            nodes: Arc::new(DashMap::new()),
            stats: Arc::new(RwLock::new(ClusterStats {
                total_nodes: 0,
                healthy_nodes: 0,
                degraded_nodes: 0,
                unreachable_nodes: 0,
                offline_nodes: 0,
                suspended_nodes: 0,
                unknown_nodes: 0,
                avg_latency_ms: None,
                avg_error_rate: 0.0,
            })),
        };

        Ok(topology)
    }

    /// Register a new node in the topology
    ///
    /// # Arguments
    ///
    /// * `node_id` - Unique node identifier
    /// * `endpoint` - Network endpoint for the node
    ///
    /// # Returns
    ///
    /// Returns true if this is a new node, false if updating existing
    pub async fn register_node(&self, node_id: String, endpoint: String) -> bool {
        let is_new = !self.nodes.contains_key(&node_id);

        if is_new {
            info!(node_id = %node_id, endpoint = %endpoint, "Registering new node");
            let status = NodeStatus::new(endpoint, node_id.clone());
            self.nodes.insert(node_id, Arc::new(RwLock::new(status)));
        } else {
            debug!(node_id = %node_id, "Node already registered");
        }

        // Update stats after registration
        if is_new {
            self.update_cluster_stats().await;
        }

        is_new
    }

    /// Update health status for a node
    ///
    /// # Arguments
    ///
    /// * `node_id` - Node identifier
    /// * `health` - New health state
    pub async fn update_node_health(&self, node_id: &str, health: NodeHealth) -> Result<()> {
        if let Some(node_ref) = self.nodes.get(node_id) {
            let mut node = node_ref.write().await;
            let old_health = node.health;
            node.set_health(health);

            if old_health != health {
                info!(
                    node_id = %node_id,
                    old_health = ?old_health,
                    new_health = ?health,
                    "Node health state changed"
                );
                drop(node); // Release lock before updating stats
                self.update_cluster_stats().await;
            }

            Ok(())
        } else {
            warn!(node_id = %node_id, "Attempted to update health for unknown node");
            anyhow::bail!("Node {} not found in topology", node_id)
        }
    }

    /// Record a successful operation to a node
    pub async fn record_node_success(&self, node_id: &str, latency_ms: u64) -> Result<()> {
        if let Some(node_ref) = self.nodes.get(node_id) {
            let mut node = node_ref.write().await;
            node.metrics.record_success(latency_ms);

            // Check if node should transition to degraded state
            let is_degraded = node
                .metrics
                .is_degraded(self.config.degraded_latency_threshold_ms, self.config.degraded_error_rate_threshold);

            if is_degraded && node.health == NodeHealth::Healthy {
                node.set_health(NodeHealth::Degraded);
                info!(node_id = %node_id, "Node transitioned to Degraded due to metrics");
            } else if !is_degraded && node.health == NodeHealth::Degraded {
                node.set_health(NodeHealth::Healthy);
                info!(node_id = %node_id, "Node recovered to Healthy state");
            }

            Ok(())
        } else {
            anyhow::bail!("Node {} not found in topology", node_id)
        }
    }

    /// Record a node operation with its result and optional latency
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    /// * `success` - Whether the operation was successful
    /// * `latency_ms` - Optional latency measurement in milliseconds
    pub async fn record_node_operation(&self, node_id: &str, success: bool, latency_ms: Option<u64>) -> Result<()> {
        if success {
            // Use latency if provided, otherwise use 0 as a placeholder
            self.record_node_success(node_id, latency_ms.unwrap_or(0)).await
        } else {
            self.record_node_failure(node_id).await
        }
    }

    /// Record a failed operation to a node
    pub async fn record_node_failure(&self, node_id: &str) -> Result<()> {
        if let Some(node_ref) = self.nodes.get(node_id) {
            let mut node = node_ref.write().await;
            node.metrics.record_failure();

            // Check if node should be marked as offline based on consecutive failures
            let failure_count = (node.metrics.request_count - node.metrics.success_count) as u32;
            let should_mark_offline = node.metrics.error_rate > 0.5 && failure_count >= self.config.failure_threshold;

            if should_mark_offline && node.health.is_operational() {
                node.set_health(NodeHealth::Offline {
                    since: chrono::Utc::now(),
                    failure_count,
                });
                warn!(node_id = %node_id, "Node marked as Offline due to consecutive failures");
            }

            Ok(())
        } else {
            anyhow::bail!("Node {} not found in topology", node_id)
        }
    }

    /// Get status of a specific node
    pub async fn get_node_status(&self, node_id: &str) -> Option<NodeStatus> {
        if let Some(node_ref) = self.nodes.get(node_id) {
            Some(node_ref.read().await.clone())
        } else {
            None
        }
    }

    /// Get all healthy nodes
    pub async fn get_healthy_nodes(&self) -> Vec<NodeStatus> {
        let mut healthy = Vec::new();

        for entry in self.nodes.iter() {
            let node = entry.value().read().await;
            if node.health == NodeHealth::Healthy {
                healthy.push(node.clone());
            }
        }

        healthy
    }

    /// Get all operational nodes (healthy + degraded)
    pub async fn get_operational_nodes(&self) -> Vec<NodeStatus> {
        let mut operational = Vec::new();

        for entry in self.nodes.iter() {
            let node = entry.value().read().await;
            if node.health.is_operational() {
                operational.push(node.clone());
            }
        }

        operational
    }

    /// Get all nodes regardless of state
    pub async fn get_all_nodes(&self) -> Vec<NodeStatus> {
        let mut all = Vec::new();

        for entry in self.nodes.iter() {
            let node = entry.value().read().await;
            all.push(node.clone());
        }

        all
    }

    /// Remove a node from topology (e.g., decommissioned)
    pub async fn remove_node(&self, node_id: &str) -> Option<NodeStatus> {
        info!(node_id = %node_id, "Removing node from topology");

        let removed = if let Some((_, node_ref)) = self.nodes.remove(node_id) {
            Some(node_ref.read().await.clone())
        } else {
            None
        };

        if removed.is_some() {
            self.update_cluster_stats().await;
        }

        removed
    }

    /// Get current cluster statistics
    pub async fn get_cluster_stats(&self) -> ClusterStats {
        self.stats.read().await.clone()
    }

    /// Update cluster-wide statistics
    ///
    /// This method recalculates stats from all nodes. Called automatically
    /// when topology changes or can be called manually for fresh stats.
    pub async fn update_cluster_stats(&self) {
        let mut total_nodes = 0;
        let mut healthy_nodes = 0;
        let mut degraded_nodes = 0;
        let mut unreachable_nodes = 0;
        let mut offline_nodes = 0;
        let mut suspended_nodes = 0;
        let mut unknown_nodes = 0;

        let mut total_latency: u64 = 0;
        let mut latency_count: u64 = 0;
        let mut total_error_rate: f64 = 0.0;

        // Collect nodes to avoid holding locks during iteration
        let mut nodes_to_process = Vec::new();
        for entry in self.nodes.iter() {
            nodes_to_process.push(Arc::clone(entry.value()));
        }

        for node_ref in nodes_to_process {
            let node = node_ref.read().await;
            total_nodes += 1;

            match node.health {
                NodeHealth::Healthy => healthy_nodes += 1,
                NodeHealth::Degraded => degraded_nodes += 1,
                NodeHealth::Unreachable => unreachable_nodes += 1,
                NodeHealth::Offline { .. } => offline_nodes += 1,
                NodeHealth::Suspended => suspended_nodes += 1,
                NodeHealth::Unknown => unknown_nodes += 1,
            }

            if let Some(latency) = node.metrics.latency_ms {
                total_latency += latency;
                latency_count += 1;
            }

            total_error_rate += node.metrics.error_rate;
        }

        let avg_latency_ms = if latency_count > 0 {
            Some(total_latency / latency_count)
        } else {
            None
        };

        let avg_error_rate = if total_nodes > 0 {
            total_error_rate / total_nodes as f64
        } else {
            0.0
        };

        let stats = ClusterStats {
            total_nodes,
            healthy_nodes,
            degraded_nodes,
            unreachable_nodes,
            offline_nodes,
            suspended_nodes,
            unknown_nodes,
            avg_latency_ms,
            avg_error_rate,
        };

        *self.stats.write().await = stats;

        debug!(
            total = total_nodes,
            healthy = healthy_nodes,
            degraded = degraded_nodes,
            offline = offline_nodes,
            "Updated cluster statistics"
        );
    }

    /// Get cluster configuration
    pub fn config(&self) -> &TopologyConfig {
        &self.config
    }

    /// Get cluster ID
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Check if cluster has write quorum
    pub async fn has_write_quorum(&self) -> bool {
        self.stats.read().await.has_write_quorum()
    }

    /// Check if cluster has read quorum
    pub async fn has_read_quorum(&self) -> bool {
        self.stats.read().await.has_read_quorum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_topology_node_registration() {
        let config = TopologyConfig::default();
        let topology = SystemTopology::new("test-cluster", config).await.unwrap();

        // Register a new node
        let is_new = topology
            .register_node("node1".to_string(), "localhost:9000".to_string())
            .await;
        assert!(is_new);

        // Register same node again
        let is_new = topology
            .register_node("node1".to_string(), "localhost:9000".to_string())
            .await;
        assert!(!is_new);

        // Verify node exists
        let status = topology.get_node_status("node1").await;
        assert!(status.is_some());
        assert_eq!(status.unwrap().endpoint, "localhost:9000");
    }

    #[tokio::test]
    async fn test_topology_health_updates() {
        let config = TopologyConfig::default();
        let topology = SystemTopology::new("test-cluster", config).await.unwrap();

        topology
            .register_node("node1".to_string(), "localhost:9000".to_string())
            .await;

        // Update health to Healthy
        topology.update_node_health("node1", NodeHealth::Healthy).await.unwrap();

        let status = topology.get_node_status("node1").await.unwrap();
        assert_eq!(status.health, NodeHealth::Healthy);

        // Get healthy nodes
        let healthy = topology.get_healthy_nodes().await;
        assert_eq!(healthy.len(), 1);
    }

    #[tokio::test]
    async fn test_topology_metrics_tracking() {
        let config = TopologyConfig::default();
        let topology = SystemTopology::new("test-cluster", config).await.unwrap();

        topology
            .register_node("node1".to_string(), "localhost:9000".to_string())
            .await;

        // Record successful operations
        topology.record_node_success("node1", 100).await.unwrap();
        topology.record_node_success("node1", 150).await.unwrap();

        let status = topology.get_node_status("node1").await.unwrap();
        assert_eq!(status.metrics.success_count, 2);
        assert_eq!(status.metrics.request_count, 2);
        assert_eq!(status.metrics.error_rate, 0.0);
    }

    #[tokio::test]
    async fn test_topology_cluster_stats() {
        let config = TopologyConfig::default();
        let topology = SystemTopology::new("test-cluster", config).await.unwrap();

        // Register multiple nodes
        topology
            .register_node("node1".to_string(), "localhost:9001".to_string())
            .await;
        topology
            .register_node("node2".to_string(), "localhost:9002".to_string())
            .await;
        topology
            .register_node("node3".to_string(), "localhost:9003".to_string())
            .await;

        // Set different health states
        topology.update_node_health("node1", NodeHealth::Healthy).await.unwrap();
        topology.update_node_health("node2", NodeHealth::Healthy).await.unwrap();
        topology
            .update_node_health(
                "node3",
                NodeHealth::Offline {
                    since: chrono::Utc::now(),
                    failure_count: 1,
                },
            )
            .await
            .unwrap();

        let stats = topology.get_cluster_stats().await;
        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.healthy_nodes, 2);
        assert_eq!(stats.offline_nodes, 1);
        assert!(stats.has_write_quorum()); // 2 > 3/2
        assert!(stats.has_read_quorum()); // 2 >= 3/2
    }
}
