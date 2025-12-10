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
use rustfs_quorum::QuorumVerifier;
use rustfs_topology::{HealthMonitor, SystemTopology, TopologyConfig};
use std::sync::{Arc, LazyLock, RwLock as StdRwLock};
use tracing::{debug, info, warn};

/// Global cluster manager instance
static GLOBAL_CLUSTER_MANAGER: LazyLock<StdRwLock<Option<Arc<ClusterManager>>>> =
    LazyLock::new(|| StdRwLock::new(None));

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
        let interval = health_check_interval_secs.unwrap_or(10);
        
        // Create topology manager
        let topology = SystemTopology::new(&cluster_id, config).await?;
        debug!("Topology manager initialized");
        
        // Create quorum verifier
        let quorum_verifier = QuorumVerifier::new(initial_node_count);
        debug!("Quorum verifier initialized for {} nodes", initial_node_count);
        
        // Create and start health monitor
        let health_monitor = HealthMonitor::new(Arc::clone(&topology), interval);
        
        let health_monitor_handle = Some(tokio::spawn(async move {
            if let Err(e) = health_monitor.start().await {
                warn!("Health monitor encountered an error: {}", e);
            }
        }));
        
        debug!("Health monitor started");
        
        let manager = Arc::new(Self {
            topology,
            quorum_verifier,
            health_monitor_handle,
            cluster_id: cluster_id.clone(),
        });
        
        // Store in global instance
        let mut global = GLOBAL_CLUSTER_MANAGER.write()
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
    pub async fn register_node(&self, node_id: String, endpoint: String) -> Result<()> {
        debug!("Registering node: {} at {}", node_id, endpoint);
        self.topology.register_node(node_id, endpoint).await
    }
    
    /// Update node health based on operation result
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    /// * `success` - Whether the operation was successful
    /// * `latency_ms` - Optional latency measurement
    pub async fn record_node_operation(
        &self,
        node_id: &str,
        success: bool,
        latency_ms: Option<u64>,
    ) -> Result<()> {
        self.topology
            .record_node_operation(node_id, success, latency_ms)
            .await
    }
    
    /// Check if write quorum is available for cluster operations
    ///
    /// Returns true if at least N/2 + 1 nodes are operational
    pub async fn check_write_quorum(&self) -> Result<bool> {
        let stats = self.topology.get_cluster_stats().await;
        self.quorum_verifier
            .check_write_quorum(stats.operational_nodes)
            .await
    }
    
    /// Check if read quorum is available for cluster operations
    ///
    /// Returns true if at least N/2 nodes are operational
    pub async fn check_read_quorum(&self) -> Result<bool> {
        let stats = self.topology.get_cluster_stats().await;
        self.quorum_verifier
            .check_read_quorum(stats.operational_nodes)
            .await
    }
    
    /// Get cluster ID
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }
    
    /// Get current cluster statistics
    pub async fn get_cluster_stats(&self) -> rustfs_topology::ClusterStats {
        self.topology.get_cluster_stats().await
    }
    
    /// Shutdown the cluster manager
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
    let manager = ClusterManager::initialize(
        cluster_id.clone(),
        node_count,
        None,
        health_check_interval_secs,
    )
    .await?;
    
    // Register all known endpoints
    for (i, endpoint) in endpoints.iter().enumerate() {
        let node_id = if let Some(fn_gen) = node_id_fn {
            fn_gen(endpoint, i)
        } else {
            // Default: extract from endpoint or use index
            format!("node-{}", i + 1)
        };
        
        manager
            .register_node(node_id, endpoint.clone())
            .await
            .unwrap_or_else(|e| {
                warn!("Failed to register node {}: {}", endpoint, e);
            });
    }
    
    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_cluster_manager_initialization() {
        let manager = ClusterManager::initialize(
            "test-cluster".to_string(),
            4,
            None,
            None,
        )
        .await
        .expect("Failed to initialize manager");
        
        assert_eq!(manager.cluster_id(), "test-cluster");
        
        // Test global instance retrieval
        let global_manager = ClusterManager::get();
        assert!(global_manager.is_some());
    }
    
    #[tokio::test]
    async fn test_node_registration() {
        let manager = ClusterManager::initialize(
            "test-cluster-2".to_string(),
            4,
            None,
            None,
        )
        .await
        .expect("Failed to initialize manager");
        
        manager
            .register_node("node1".to_string(), "http://node1:9000".to_string())
            .await
            .expect("Failed to register node");
        
        let stats = manager.get_cluster_stats().await;
        assert!(stats.total_nodes >= 1);
    }
    
    #[tokio::test]
    async fn test_quorum_checks_with_nodes() {
        let manager = ClusterManager::initialize(
            "test-cluster-3".to_string(),
            4,
            None,
            None,
        )
        .await
        .expect("Failed to initialize manager");
        
        // Register nodes
        for i in 1..=4 {
            manager
                .register_node(
                    format!("node{}", i),
                    format!("http://node{}:9000", i),
                )
                .await
                .expect("Failed to register node");
        }
        
        // Check quorum (all nodes healthy initially)
        let write_quorum = manager.check_write_quorum().await.expect("Quorum check failed");
        let read_quorum = manager.check_read_quorum().await.expect("Quorum check failed");
        
        assert!(write_quorum, "Write quorum should be available with 4 healthy nodes");
        assert!(read_quorum, "Read quorum should be available with 4 healthy nodes");
    }
    
    #[tokio::test]
    async fn test_node_operation_recording() {
        let manager = ClusterManager::initialize(
            "test-cluster-4".to_string(),
            2,
            None,
            None,
        )
        .await
        .expect("Failed to initialize manager");
        
        manager
            .register_node("node1".to_string(), "http://node1:9000".to_string())
            .await
            .expect("Failed to register node");
        
        // Record successful operation
        manager
            .record_node_operation("node1", true, Some(50))
            .await
            .expect("Failed to record operation");
        
        let stats = manager.get_cluster_stats().await;
        assert!(stats.healthy_nodes >= 1);
    }
}
