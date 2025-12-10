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

//! Cluster Manager - Integration module for topology and quorum management
//!
//! This module integrates the new topology and quorum crates with the existing
//! RustFS cluster infrastructure, providing seamless coordination between:
//! - Global topology state management
//! - Quorum-based cluster operations  
//! - Connection health tracking
//! - EC quorum management

use anyhow::Result;
use futures_util::TryFutureExt;
use rustfs_quorum::QuorumVerifier;
use rustfs_topology::{HealthMonitor, SystemTopology, TopologyConfig};
use std::sync::{Arc, LazyLock, RwLock as StdRwLock};
use tracing::{debug, info, warn};

/// Global cluster manager instance
static GLOBAL_CLUSTER_MANAGER: LazyLock<StdRwLock<Option<Arc<ClusterManager>>>> = LazyLock::new(|| StdRwLock::new(None));

/// Cluster manager coordinating all cluster resilience components
pub struct ClusterManager {
    /// Topology state management
    pub topology: Arc<SystemTopology>,

    /// Quorum verifier for cluster operations
    pub quorum_verifier: Arc<QuorumVerifier>,

    /// Health monitor service handle
    health_monitor_handle: Option<tokio::task::JoinHandle<()>>,

    /// Cluster ID
    cluster_id: String,
}

impl ClusterManager {
    /// Initialize the cluster manager
    ///
    /// This should be called during server startup to initialize the global
    /// topology and quorum management systems.
    ///
    /// # Arguments
    /// * `cluster_id` - Unique cluster identifier
    /// * `initial_node_count` - Expected number of nodes in the cluster
    /// * `config` - Optional topology configuration (uses defaults if None)
    /// * `health_check_interval_secs` - Health monitor check interval in seconds (default: 10)
    pub async fn initialize(
        cluster_id: String,
        initial_node_count: usize,
        config: Option<TopologyConfig>,
        health_check_interval_secs: Option<u64>,
    ) -> Result<Arc<Self>> {
        info!("Initializing Cluster Manager for cluster: {}", cluster_id);

        let config = config.unwrap_or_default();
        let _interval = health_check_interval_secs.unwrap_or(10);

        // Create topology manager
        let topology = SystemTopology::new(&cluster_id, config).await?;
        debug!("Topology manager initialized");

        // Create quorum verifier
        let quorum_verifier = QuorumVerifier::new(initial_node_count);
        debug!("Quorum verifier initialized for {} nodes", initial_node_count);

        // Create and start health monitor
        let health_monitor = HealthMonitor::new(Arc::new(topology));
        let mut health_monitor_clone = Arc::new(health_monitor);
        let health_monitor_handle = Some(tokio::spawn(async move {
            if let Err(e) = health_monitor_clone.start() {
                warn!("Health monitor encountered an error: {}", e);
            }
        }));

        debug!("Health monitor started");

        let manager = Arc::new(Self {
            topology: topology.into(),
            quorum_verifier: quorum_verifier.into(),
            health_monitor_handle,
            cluster_id: cluster_id.clone(),
        });

        // Store in global instance
        let mut global = GLOBAL_CLUSTER_MANAGER
            .write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire global lock: {}", e))?;
        *global = Some(Arc::clone(&manager));

        info!("Cluster Manager initialized successfully");
        Ok(manager)
    }

    /// Get the global cluster manager instance
    pub fn get() -> Option<Arc<Self>> {
        GLOBAL_CLUSTER_MANAGER.read().ok()?.clone()
    }

    /// Register a node in the topology
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for the node
    /// * `endpoint` - Network endpoint (e.g., "http://node1:9000")
    pub async fn register_node(&self, node_id: &str, endpoint: &str) -> bool {
        debug!("Registering node: {} at {}", node_id, endpoint);
        self.topology.register_node(node_id.to_string(), endpoint.to_string()).await
    }

    /// Update node health based on operation result
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    /// * `success` - Whether the operation was successful
    /// * `latency_ms` - Optional latency measurement
    pub async fn record_node_operation(&self, node_id: &str, success: bool, latency_ms: Option<u64>) -> Result<()> {
        self.topology.record_node_operation(node_id, success, latency_ms).await
    }

    /// Check if write quorum is available for cluster operations
    ///
    /// Returns true if at least N/2 + 1 nodes are operational
    pub async fn check_write_quorum(&self) -> Result<bool> {
        let stats = self.topology.get_cluster_stats().await;
        self.quorum_verifier.check_write_quorum(stats.operational_nodes).await
    }

    /// Check if read quorum is available for cluster operations
    ///
    /// Returns true if at least N/2 nodes are operational
    pub async fn check_read_quorum(&self) -> Result<bool> {
        let stats = self.topology.get_cluster_stats().await;
        self.quorum_verifier.check_read_quorum(stats.operational_nodes).await
    }

    /// Get cluster ID
    pub fn get_cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Get current cluster statistics
    pub async fn get_cluster_stats(&self) -> rustfs_topology::ClusterStats {
        self.topology.get_cluster_stats().await
    }

    /// Shutdown the cluster manager
    ///
    /// Note: This method requires exclusive ownership. Consider using Arc::try_unwrap
    /// or other patterns to get mutable access from the shared Arc instance.
    pub async fn shutdown(&mut self) {
        info!("Shutting down Cluster Manager");

        // Stop health monitor
        if let Some(handle) = self.health_monitor_handle.take() {
            handle.abort();
            debug!("Health monitor stopped");
        }

        info!("Cluster Manager shutdown complete");
    }
}

impl Drop for ClusterManager {
    fn drop(&mut self) {
        // Ensure health monitor is stopped
        if let Some(handle) = self.health_monitor_handle.take() {
            handle.abort();
        }
    }
}

/// Initialize cluster manager with automatic node discovery
///
/// This is a convenience function for integration with existing RustFS initialization.
///
/// # Arguments
/// * `cluster_id` - Unique cluster identifier
/// * `endpoints` - List of node endpoints in the cluster
/// * `node_id_fn` - Optional function to generate node IDs from endpoints
/// * `health_check_interval_secs` - Optional health check interval (default: 10s)
pub async fn initialize_cluster_management(
    cluster_id: String,
    endpoints: &[String],
    node_id_fn: Option<&dyn Fn(&str, usize) -> String>,
    health_check_interval_secs: Option<u64>,
) -> Result<Arc<ClusterManager>> {
    let node_count = endpoints.len().max(1);
    let manager = ClusterManager::initialize(cluster_id.clone(), node_count, None, health_check_interval_secs).await?;

    // Register all known endpoints
    for (i, endpoint) in endpoints.iter().enumerate() {
        let node_id = if let Some(fn_gen) = node_id_fn {
            fn_gen(endpoint, i)
        } else {
            // Default: extract from endpoint or use index
            format!("node-{}", i + 1)
        };

        manager.register_node(&node_id, endpoint).await.unwrap_or_else(|e| {
            warn!("Failed to register node {}: {}", endpoint, e);
        });
    }

    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_topology::{NodeHealth, TopologyConfig};
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_cluster_manager_initialization() {
        let manager = ClusterManager::initialize("test-cluster".to_string(), 4, None, None)
            .await
            .expect("Failed to initialize manager");

        assert_eq!(manager.get_cluster_id(), "test-cluster");

        // Test global instance retrieval
        let global_manager = ClusterManager::get();
        assert!(global_manager.is_some());
    }

    #[tokio::test]
    async fn test_node_registration() {
        let manager = ClusterManager::initialize("test-cluster-2".to_string(), 4, None, None)
            .await
            .expect("Failed to initialize manager");

        manager.register_node("node1", "http://node1:9000").await;

        let stats = manager.get_cluster_stats().await;
        assert!(stats.total_nodes >= 1);
    }

    #[tokio::test]
    async fn test_quorum_checks_with_nodes() {
        let manager = ClusterManager::initialize("test-cluster-3".to_string(), 4, None, None)
            .await
            .expect("Failed to initialize manager");

        // Register nodes
        for i in 1..=4 {
            manager
                .register_node(&format!("node{}", i), &format!("http://node{}:9000", i))
                .await;
        }

        // Check quorum (all nodes healthy initially)
        let write_quorum = manager.check_write_quorum().await.expect("Quorum check failed");
        let read_quorum = manager.check_read_quorum().await.expect("Quorum check failed");

        assert!(write_quorum, "Write quorum should be available with 4 healthy nodes");
        assert!(read_quorum, "Read quorum should be available with 4 healthy nodes");
    }

    #[tokio::test]
    async fn test_node_operation_recording() {
        let manager = ClusterManager::initialize("test-cluster-4".to_string(), 2, None, None)
            .await
            .expect("Failed to initialize manager");

        manager.register_node("node1", "http://node1:9000").await;

        // Record successful operation
        manager
            .record_node_operation("node1", true, Some(50))
            .await
            .expect("Failed to record operation");

        let stats = manager.get_cluster_stats().await;
        assert!(stats.healthy_nodes >= 1);
    }

    /// Test basic cluster initialization
    #[tokio::test]
    async fn test_cluster_initialization() {
        let cluster_id = "test-cluster-init";
        let total_nodes = 4;
        let config = TopologyConfig::default();

        let mut manager = ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), None)
            .await
            .expect("Failed to initialize cluster manager");

        // Verify cluster ID
        assert_eq!(manager.get_cluster_id(), cluster_id);

        // Verify stats - initially no nodes registered
        let stats = manager.get_cluster_stats().await;
        assert_eq!(stats.total_nodes, 0);
        assert_eq!(stats.healthy_nodes, 0);

        manager.shutdown().await;
    }

    /// Test node registration and discovery
    #[tokio::test]
    async fn test_node_registration() {
        let cluster_id = "test-cluster-registration";
        let total_nodes = 4;
        let config = TopologyConfig::default();

        let mut manager = ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), None)
            .await
            .expect("Failed to initialize cluster manager");

        // Register nodes
        manager.register_node("node1", "192.168.1.1:9000").await;
        manager.register_node("node2", "192.168.1.2:9000").await;
        manager.register_node("node3", "192.168.1.3:9000").await;

        // Verify registration
        let stats = manager.get_cluster_stats().await;
        assert_eq!(stats.total_nodes, 3);
        // New nodes start as Unknown, not Healthy
        assert_eq!(stats.healthy_nodes, 0);

        manager.shutdown().await;
    }

    /// Test node health tracking and state transitions
    #[tokio::test]
    async fn test_node_health_tracking() {
        let cluster_id = "test-cluster-health";
        let total_nodes = 4;
        let config = TopologyConfig::default();

        let mut manager = ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), None)
            .await
            .expect("Failed to initialize cluster manager");

        // Register node
        manager.register_node("node1", "192.168.1.1:9000").await;

        // Record successful operations - should transition to Healthy
        manager
            .record_node_operation("node1", true, Some(10))
            .await
            .expect("Failed to record success");
        manager
            .record_node_operation("node1", true, Some(12))
            .await
            .expect("Failed to record success");

        // Verify health state
        let node_status = manager.topology.get_node_status("node1").await;
        assert!(node_status.is_some());
        let status = node_status.unwrap();
        assert_eq!(status.health, NodeHealth::Healthy);
        assert_eq!(status.metrics.success_count, 2);

        // Record failures - should transition to Degraded or Offline
        for _ in 0..5 {
            manager
                .record_node_operation("node1", false, None)
                .await
                .expect("Failed to record failure");
        }

        // Verify degraded/offline state
        let node_status = manager.topology.get_node_status("node1").await;
        assert!(node_status.is_some());
        let status = node_status.unwrap();
        // After 5 failures, should be degraded or offline
        assert_ne!(status.health, NodeHealth::Healthy);

        manager.shutdown().await;
    }

    /// Test write quorum verification
    #[tokio::test]
    async fn test_write_quorum() {
        let cluster_id = "test-cluster-write-quorum";
        let total_nodes = 4; // Write quorum = 3 (N/2 + 1)
        let config = TopologyConfig::default();

        let mut manager = ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), None)
            .await
            .expect("Failed to initialize cluster manager");

        // Register nodes
        for i in 1..=4 {
            manager
                .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
                .await;
        }

        // Mark 3 nodes as healthy (meets write quorum)
        for i in 1..=3 {
            manager
                .record_node_operation(&format!("node{}", i), true, Some(10))
                .await
                .expect("Failed to record success");
        }

        // Check write quorum - should pass with 3 healthy nodes
        let can_write = manager.check_write_quorum().await.expect("Failed to check write quorum");
        assert!(can_write, "Write quorum should be satisfied with 3/4 nodes");

        // Mark one more node as failed (only 2 healthy left)
        for _ in 0..5 {
            manager
                .record_node_operation("node3", false, None)
                .await
                .expect("Failed to record failure");
        }

        // Wait a bit for state to update
        sleep(Duration::from_millis(100)).await;

        // Check write quorum - should fail with only 2 healthy nodes
        let can_write = manager.check_write_quorum().await.expect("Failed to check write quorum");
        assert!(!can_write, "Write quorum should NOT be satisfied with only 2/4 nodes");

        manager.shutdown().await;
    }

    /// Test read quorum verification
    #[tokio::test]
    async fn test_read_quorum() {
        let cluster_id = "test-cluster-read-quorum";
        let total_nodes = 4; // Read quorum = 2 (N/2)
        let config = TopologyConfig::default();

        let mut manager = ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), None)
            .await
            .expect("Failed to initialize cluster manager");

        // Register nodes
        for i in 1..=4 {
            manager
                .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
                .await;
        }

        // Mark 2 nodes as healthy (meets read quorum)
        for i in 1..=2 {
            manager
                .record_node_operation(&format!("node{}", i), true, Some(10))
                .await
                .expect("Failed to record success");
        }

        // Check read quorum - should pass with 2 healthy nodes
        let can_read = manager.check_read_quorum().await.expect("Failed to check read quorum");
        assert!(can_read, "Read quorum should be satisfied with 2/4 nodes");

        // Mark one more node as failed (only 1 healthy left)
        for _ in 0..5 {
            manager
                .record_node_operation("node2", false, None)
                .await
                .expect("Failed to record failure");
        }

        // Wait a bit for state to update
        sleep(Duration::from_millis(100)).await;

        // Check read quorum - should fail with only 1 healthy node
        let can_read = manager.check_read_quorum().await.expect("Failed to check read quorum");
        assert!(!can_read, "Read quorum should NOT be satisfied with only 1/4 nodes");

        manager.shutdown().await;
    }

    /// Test node failure detection and recovery
    #[tokio::test]
    async fn test_node_failure_recovery() {
        let cluster_id = "test-cluster-recovery";
        let total_nodes = 3;
        let config = TopologyConfig {
            health_check_timeout_secs: 1, // Short timeout for testing
            ..Default::default()
        };

        let mut manager = ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), Some(1))
            .await
            .expect("Failed to initialize cluster manager");

        // Register nodes
        for i in 1..=3 {
            manager
                .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
                .await;
            // Mark as healthy initially
            manager
                .record_node_operation(&format!("node{}", i), true, Some(10))
                .await
                .expect("Failed to record success");
        }

        // Verify all healthy
        let stats = manager.get_cluster_stats().await;
        assert_eq!(stats.healthy_nodes, 3);

        // Simulate node failure
        for _ in 0..5 {
            manager
                .record_node_operation("node2", false, None)
                .await
                .expect("Failed to record failure");
        }

        // Wait for health monitor to detect
        sleep(Duration::from_secs(2)).await;

        // Verify failure detected
        let stats = manager.get_cluster_stats().await;
        assert!(stats.healthy_nodes < 3, "Failed node should be detected by health monitor");

        // Simulate recovery
        for _ in 0..5 {
            manager
                .record_node_operation("node2", true, Some(10))
                .await
                .expect("Failed to record success");
        }

        // Wait for recovery detection
        sleep(Duration::from_secs(2)).await;

        // Verify recovery
        let stats = manager.get_cluster_stats().await;
        assert_eq!(stats.healthy_nodes, 3, "Recovered node should be marked healthy");

        manager.shutdown().await;
    }

    /// Test concurrent node operations
    #[tokio::test]
    async fn test_concurrent_operations() {
        let cluster_id = "test-cluster-concurrent";
        let total_nodes = 4;
        let config = TopologyConfig::default();

        let mut manager = Arc::new(
            ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), None)
                .await
                .expect("Failed to initialize cluster manager"),
        );

        // Register nodes
        for i in 1..=4 {
            manager
                .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
                .await;
        }

        // Spawn concurrent operations on different nodes
        let mut handles = vec![];
        for node_id in 1..=4 {
            let mgr = Arc::clone(&manager);
            let handle = tokio::spawn(async move {
                for _ in 0..10 {
                    let _ = mgr.record_node_operation(&format!("node{}", node_id), true, Some(10)).await;
                    sleep(Duration::from_millis(10)).await;
                }
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.expect("Task failed");
        }

        // Verify no crashes and state is consistent
        let stats = manager.get_cluster_stats().await;
        assert_eq!(stats.total_nodes, 4);

        manager.shutdown().await;
    }

    /// Test cluster stats calculation
    #[tokio::test]
    async fn test_cluster_stats() {
        let cluster_id = "test-cluster-stats";
        let total_nodes = 5;
        let config = TopologyConfig::default();

        let mut manager = ClusterManager::initialize(cluster_id.to_string(), total_nodes, Some(config), None)
            .await
            .expect("Failed to initialize cluster manager");

        // Register 5 nodes with different health states
        for i in 1..=5 {
            manager
                .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
                .await;
        }

        // Mark 3 as healthy
        for i in 1..=3 {
            manager
                .record_node_operation(&format!("node{}", i), true, Some(10))
                .await
                .expect("Failed to record success");
        }

        // Mark 1 as degraded (some failures)
        manager
            .record_node_operation("node4", false, None)
            .await
            .expect("Failed to record failure");
        manager
            .record_node_operation("node4", true, Some(50))
            .await
            .expect("Failed to record success"); // High latency

        // Mark 1 as offline (many failures)
        for _ in 0..5 {
            manager
                .record_node_operation("node5", false, None)
                .await
                .expect("Failed to record failure");
        }

        // Get stats
        let stats = manager.get_cluster_stats().await;

        // Verify counts
        assert_eq!(stats.total_nodes, 5);
        assert_eq!(stats.healthy_nodes, 3);
        assert!(stats.has_write_quorum()); // 3 >= (5/2 + 1) = 3
        assert!(stats.has_read_quorum()); // 3 >= (5/2) = 2

        manager.shutdown().await;
    }
}
