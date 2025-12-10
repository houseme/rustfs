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

//! Health monitoring service for cluster nodes
//!
//! Performs periodic health checks on all nodes in the topology and updates
//! their health states based on availability and performance metrics.

use crate::topology::SystemTopology;
use crate::types::NodeHealth;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

/// Health monitoring service that performs periodic health checks
///
/// This service runs in the background and periodically checks the health
/// of all nodes in the topology, updating their states as needed.
pub struct HealthMonitor {
    /// Reference to the topology being monitored
    topology: Arc<SystemTopology>,

    /// Health check interval
    check_interval: Duration,

    /// Handle to the background task
    task_handle: Option<JoinHandle<()>>,
}

impl HealthMonitor {
    /// Create a new health monitor
    ///
    /// # Arguments
    ///
    /// * `topology` - The SystemTopology to monitor
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use rustfs_topology::{SystemTopology, TopologyConfig, HealthMonitor};
    ///
    /// #[tokio::main]
    /// async fn main() -> anyhow::Result<()> {
    ///     let config = TopologyConfig::default();
    ///     let topology = Arc::new(SystemTopology::new("cluster-1", config).await?);
    ///     let monitor = HealthMonitor::new(topology);
    ///     Ok(())
    /// }
    /// ```
    pub fn new(topology: Arc<SystemTopology>) -> Self {
        let check_interval = Duration::from_secs(topology.config().health_check_interval_secs);

        Self {
            topology,
            check_interval,
            task_handle: None,
        }
    }

    /// Start the health monitoring service
    ///
    /// Spawns a background task that periodically checks node health.
    /// Returns immediately after spawning the task.
    pub fn start(&mut self) {
        info!(interval_secs = self.check_interval.as_secs(), "Starting health monitor service");

        let topology = Arc::clone(&self.topology);
        let check_interval = self.check_interval;

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(check_interval).await;

                match Self::perform_health_check(&topology).await {
                    Ok(checked) => {
                        debug!(nodes_checked = checked, "Health check cycle completed");
                    }
                    Err(e) => {
                        error!(error = %e, "Health check cycle failed");
                    }
                }
            }
        });

        self.task_handle = Some(handle);
    }

    /// Perform a single health check cycle on all nodes
    ///
    /// This is called periodically by the background task but can also
    /// be called manually for on-demand health checks.
    ///
    /// # Returns
    ///
    /// Number of nodes checked
    pub async fn perform_health_check(topology: &Arc<SystemTopology>) -> anyhow::Result<usize> {
        let nodes = topology.get_all_nodes().await;
        let total_nodes = nodes.len();

        if total_nodes == 0 {
            debug!("No nodes to health check");
            return Ok(0);
        }

        debug!(total_nodes = total_nodes, "Starting health check cycle");

        let mut checked = 0;

        for node in nodes {
            // For each node, we would normally perform actual health check
            // via HTTP/gRPC ping. For now, we update based on existing metrics.

            // Simulate health check based on last successful operation time
            if let Some(last_success) = node.metrics.last_success {
                let elapsed = chrono::Utc::now() - last_success;
                let elapsed_secs = elapsed.num_seconds() as u64;

                // If no successful operation in 2x the check interval, mark as unreachable
                let timeout_threshold = topology.config().health_check_interval_secs * 2;

                if elapsed_secs > timeout_threshold && node.health.is_operational() {
                    debug!(
                        node_id = %node.node_id,
                        elapsed_secs = elapsed_secs,
                        "No recent activity, marking as unreachable"
                    );

                    if let Err(e) = topology.update_node_health(&node.node_id, NodeHealth::Unreachable).await {
                        error!(
                            node_id = %node.node_id,
                            error = %e,
                            "Failed to update node health"
                        );
                    }
                }
            }

            checked += 1;
        }

        // Update cluster stats after health check
        topology.update_cluster_stats().await;

        Ok(checked)
    }

    /// Stop the health monitoring service
    ///
    /// Aborts the background health check task.
    pub fn stop(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            info!("Stopping health monitor service");
            handle.abort();
        }
    }
}

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::SystemTopology;
    use crate::types::TopologyConfig;

    #[tokio::test]
    async fn test_health_monitor_creation() {
        let config = TopologyConfig::default();
        let topology = Arc::new(SystemTopology::new("test-cluster", config).await.unwrap());
        let monitor = HealthMonitor::new(topology);

        assert!(monitor.task_handle.is_none());
    }

    #[tokio::test]
    async fn test_health_monitor_start_stop() {
        let config = TopologyConfig::default();
        let topology = Arc::new(SystemTopology::new("test-cluster", config).await.unwrap());
        let mut monitor = HealthMonitor::new(topology);

        // Start monitoring
        monitor.start();
        assert!(monitor.task_handle.is_some());

        // Stop monitoring
        monitor.stop();
        assert!(monitor.task_handle.is_none());
    }

    #[tokio::test]
    async fn test_health_check_cycle() {
        let config = TopologyConfig::default();
        let topology = Arc::new(SystemTopology::new("test-cluster", config).await.unwrap());

        // Register some nodes
        topology
            .register_node("node1".to_string(), "localhost:9001".to_string())
            .await;
        topology
            .register_node("node2".to_string(), "localhost:9002".to_string())
            .await;

        // Perform health check
        let checked = HealthMonitor::perform_health_check(&topology).await.unwrap();

        assert_eq!(checked, 2);
    }
}
