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
use anyhow::{Result, anyhow};
use chrono::Utc;
use hashbrown::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Global topology state manager for RustFS cluster
///
/// Maintains centralized view of all nodes in the cluster with their health
/// states, metrics, and metadata. Provides fast lock-free reads for hot paths.
/// Centralized topology state that uses hashbrown + async RwLocks for predictable lock scopes.
#[derive(Debug, Clone)]
pub struct SystemTopology {
    /// Cluster identifier
    cluster_id: String,

    /// Configuration
    config: TopologyConfig,

    /// Concurrent map of node_id -> NodeStatus
    /// DashMap allows lock-free concurrent reads
    nodes: Arc<RwLock<HashMap<String, Arc<RwLock<NodeStatus>>>>>,

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
            nodes: Arc::new(RwLock::new(HashMap::new())),
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
        let mut nodes = self.nodes.write().await;
        if nodes.contains_key(&node_id) {
            debug!(node_id = %node_id, "Node already registered");
            return false;
        }

        info!(node_id = %node_id, endpoint = %endpoint, "Registering new node");
        let status = NodeStatus::new(endpoint, node_id.clone());
        nodes.insert(node_id, Arc::new(RwLock::new(status)));
        drop(nodes);

        self.update_cluster_stats().await;
        true
    }

    /// Update health status for a node
    ///
    /// # Arguments
    ///
    /// * `node_id` - Node identifier
    /// * `health` - New health state
    pub async fn update_node_health(&self, node_id: &str, health: NodeHealth) -> Result<()> {
        let node_lock = self.require_node_lock(node_id).await?;
        let mut node = node_lock.write().await;
        let old_health = node.health;
        node.set_health(health);
        let changed = old_health != node.health;
        drop(node);

        if changed {
            info!(
                node_id = %node_id,
                old = ?old_health,
                new = ?health,
                "Node health changed explicitly"
            );
            self.update_cluster_stats().await;
        }

        Ok(())
    }

    /// Record a successful operation to a node
    pub async fn record_node_success(&self, node_id: &str, latency_ms: u64) -> Result<()> {
        let node_lock = self.require_node_lock(node_id).await?;
        let mut node = node_lock.write().await;
        node.metrics.record_success(latency_ms);

        let desired = Self::evaluate_operational_health(&node.metrics, &self.config);
        let mutated = Self::apply_health_transition(node_id, &mut node, desired);
        drop(node);

        if mutated {
            self.update_cluster_stats().await;
        }

        Ok(())
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
        let node_lock = self.require_node_lock(node_id).await?;
        let mut node = node_lock.write().await;
        node.metrics.record_failure();

        let failure_count = node.metrics.request_count.saturating_sub(node.metrics.success_count) as u32;
        let should_offline = node.metrics.error_rate > 0.5 && failure_count >= self.config.failure_threshold;

        let desired = if should_offline {
            NodeHealth::Offline {
                since: Utc::now(),
                failure_count,
            }
        } else {
            Self::evaluate_operational_health(&node.metrics, &self.config)
        };

        let mutated = Self::apply_health_transition(node_id, &mut node, desired);
        drop(node);

        if mutated {
            self.update_cluster_stats().await;
        }

        Ok(())
    }

    /// Get status of a specific node
    pub async fn get_node_status(&self, node_id: &str) -> Option<NodeStatus> {
        let handle = {
            let nodes = self.nodes.read().await;
            nodes.get(node_id).cloned()
        }?;

        Some(handle.read().await.clone())
    }

    /// Get all healthy nodes
    pub async fn get_healthy_nodes(&self) -> Vec<NodeStatus> {
        self.collect_nodes_matching(|status| status.health == NodeHealth::Healthy)
            .await
    }

    /// Get all operational nodes (healthy + degraded)
    pub async fn get_operational_nodes(&self) -> Vec<NodeStatus> {
        self.collect_nodes_matching(|status| status.health.is_operational()).await
    }

    /// Get all nodes regardless of state
    pub async fn get_all_nodes(&self) -> Vec<NodeStatus> {
        self.collect_nodes_matching(|_| true).await
    }

    /// Remove a node from topology (e.g., decommissioned)
    pub async fn remove_node(&self, node_id: &str) -> Option<NodeStatus> {
        info!(node_id = %node_id, "Removing node from topology");

        let removed = {
            let mut nodes = self.nodes.write().await;
            nodes.remove(node_id)
        }?;

        let snapshot = removed.read().await.clone();
        drop(removed);

        self.update_cluster_stats().await;
        Some(snapshot)
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
        let handles = {
            let nodes = self.nodes.read().await;
            nodes.values().cloned().collect::<Vec<_>>()
        };

        let mut stats = ClusterStats {
            total_nodes: 0,
            healthy_nodes: 0,
            degraded_nodes: 0,
            unreachable_nodes: 0,
            offline_nodes: 0,
            suspended_nodes: 0,
            unknown_nodes: 0,
            avg_latency_ms: None,
            avg_error_rate: 0.0,
        };

        let mut latency_acc: u64 = 0;
        let mut latency_count: u64 = 0;
        let mut aggregated_error_rate = 0.0;

        for handle in handles {
            let node = handle.read().await;
            stats.total_nodes += 1;

            match node.health {
                NodeHealth::Healthy => stats.healthy_nodes += 1,
                NodeHealth::Degraded => stats.degraded_nodes += 1,
                NodeHealth::Unreachable => stats.unreachable_nodes += 1,
                NodeHealth::Offline { .. } => stats.offline_nodes += 1,
                NodeHealth::Suspended => stats.suspended_nodes += 1,
                NodeHealth::Unknown => stats.unknown_nodes += 1,
            }

            if let Some(latency) = node.metrics.latency_ms {
                latency_acc += latency;
                latency_count += 1;
            }

            aggregated_error_rate += node.metrics.error_rate;
        }

        stats.avg_latency_ms = if latency_count > 0 {
            Some(latency_acc / latency_count)
        } else {
            None
        };

        stats.avg_error_rate = if stats.total_nodes > 0 {
            aggregated_error_rate / stats.total_nodes as f64
        } else {
            0.0
        };

        *self.stats.write().await = stats.clone();

        debug!(
            total = stats.total_nodes,
            healthy = stats.healthy_nodes,
            degraded = stats.degraded_nodes,
            offline = stats.offline_nodes,
            "Cluster statistics updated"
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

    async fn require_node_lock(&self, node_id: &str) -> Result<Arc<RwLock<NodeStatus>>> {
        let nodes = self.nodes.read().await;
        nodes
            .get(node_id)
            .cloned()
            .ok_or_else(|| anyhow!("Node {} not found in topology", node_id))
    }

    async fn collect_nodes_matching<F>(&self, predicate: F) -> Vec<NodeStatus>
    where
        F: Fn(&NodeStatus) -> bool,
    {
        let handles = {
            let nodes = self.nodes.read().await;
            nodes.values().cloned().collect::<Vec<_>>()
        };

        let mut gathered = Vec::with_capacity(handles.len());
        for handle in handles {
            let node = handle.read().await;
            if predicate(&node) {
                gathered.push(node.clone());
            }
        }
        gathered
    }

    fn evaluate_operational_health(metrics: &NodeMetrics, config: &TopologyConfig) -> NodeHealth {
        if metrics.request_count == 0 {
            return NodeHealth::Unknown;
        }

        let latency_flag = metrics
            .latency_ms
            .map(|latency| latency > config.degraded_latency_threshold_ms)
            .unwrap_or(false);

        if latency_flag || metrics.error_rate > config.degraded_error_rate_threshold {
            NodeHealth::Degraded
        } else {
            NodeHealth::Healthy
        }
    }

    fn apply_health_transition(node_id: &str, node: &mut NodeStatus, desired: NodeHealth) -> bool {
        if node.health == desired {
            return false;
        }

        info!(
            node_id = %node_id,
            old = ?node.health,
            new = ?desired,
            "Node health transition triggered by metrics"
        );
        node.set_health(desired);
        true
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
        assert!(
            topology
                .register_node("node1".to_string(), "localhost:9000".to_string())
                .await
        );

        // Register same node again
        assert!(
            !topology
                .register_node("node1".to_string(), "localhost:9000".to_string())
                .await
        );

        // Verify node exists
        let status = topology.get_node_status("node1").await.unwrap();
        assert_eq!(status.endpoint, "localhost:9000");
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
                    since: Utc::now(),
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

    #[tokio::test]
    async fn test_failure_escalation_to_offline() {
        let config = TopologyConfig::default();
        let topology = SystemTopology::new("test-cluster", config).await.unwrap();

        topology
            .register_node("node1".to_string(), "localhost:9000".to_string())
            .await;

        for _ in 0..5 {
            topology.record_node_failure("node1").await.unwrap();
        }

        let status = topology.get_node_status("node1").await.unwrap();
        assert!(matches!(status.health, NodeHealth::Offline { .. }));
    }
}
