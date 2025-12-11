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

//! Metrics collection for topology - placeholder for future development
//!
//! `MetricsCollector` each snapshot not only pushes data through the watch channel, but also
//! Use the `metrics` crate to report aggregated metrics (total number of nodes, availability, number of each health status, etc.),
//! Convenient for Prometheus/OTLP and other backends to pull.

use crate::topology::SystemTopology;
use crate::types::{ClusterStats, NodeHealth};
use chrono::{DateTime, Utc};
use metrics::{gauge, histogram};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info};

const TOP_NODE_SAMPLE: usize = 5;

/// Lightweight view over per-node metrics suitable for dashboards.
#[derive(Debug, Clone)]
pub struct NodeMetricPoint {
    pub node_id: String,
    pub health: NodeHealth,
    pub latency_ms: Option<u64>,
    pub error_rate: f64,
}

/// Snapshot produced by the metrics collector loop.
#[derive(Debug, Clone)]
pub struct ClusterMetricsSnapshot {
    pub captured_at: DateTime<Utc>,
    pub stats: ClusterStats,
    pub top_nodes: Vec<NodeMetricPoint>,
}

/// Periodic metrics collector that streams snapshots through a watch channel.
#[derive(Debug)]
pub struct MetricsCollector {
    topology: Arc<SystemTopology>,
    interval: Duration,
    sender: watch::Sender<ClusterMetricsSnapshot>,
    task: Option<JoinHandle<()>>,
}

impl MetricsCollector {
    /// Build a collector and seed it with an initial snapshot.
    pub async fn new(topology: Arc<SystemTopology>, interval_secs: u64) -> Self {
        let snapshot = Self::capture_snapshot(&topology).await;
        let (sender, _) = watch::channel(snapshot);

        Self {
            topology,
            interval: Duration::from_secs(interval_secs.max(1)),
            sender,
            task: None,
        }
    }

    /// Start the background task if not running yet.
    pub fn start(&mut self) {
        if self.task.is_some() {
            return;
        }

        info!(interval = self.interval.as_secs(), "Starting metrics collector");
        let topology = Arc::clone(&self.topology);
        let interval = self.interval;
        let sender = self.sender.clone();

        let handle = tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let snapshot = Self::capture_snapshot(&topology).await;
                if sender.send(snapshot).is_err() {
                    debug!("All metrics subscribers dropped; stopping collector");
                    break;
                }
            }
        });

        self.task = Some(handle);
    }

    /// Stop the background task and drop the sender.
    pub fn stop(&mut self) {
        if let Some(handle) = self.task.take() {
            info!("Stopping metrics collector");
            handle.abort();
        }
    }

    /// Subscribe to the watch channel and receive future snapshots.
    pub fn subscribe(&self) -> watch::Receiver<ClusterMetricsSnapshot> {
        self.sender.subscribe()
    }

    async fn capture_snapshot(topology: &Arc<SystemTopology>) -> ClusterMetricsSnapshot {
        let stats = topology.get_cluster_stats().await;
        let mut nodes = topology.get_all_nodes().await;

        nodes.sort_by(|a, b| {
            let a_latency = a.metrics.latency_ms.unwrap_or(u64::MAX);
            let b_latency = b.metrics.latency_ms.unwrap_or(u64::MAX);
            a_latency.cmp(&b_latency)
        });

        let top_nodes = nodes
            .into_iter()
            .take(TOP_NODE_SAMPLE)
            .map(|node| NodeMetricPoint {
                node_id: node.node_id.clone(),
                health: node.health,
                latency_ms: node.metrics.latency_ms,
                error_rate: node.metrics.error_rate,
            })
            .collect();

        // Emit metrics via `metrics` crate
        gauge!("rustfs_topology_nodes_total").increment(stats.total_nodes as f64);
        gauge!("rustfs_topology_nodes_healthy").increment(stats.healthy_nodes as f64);
        gauge!("rustfs_topology_nodes_degraded").increment(stats.degraded_nodes as f64);
        gauge!("rustfs_topology_nodes_unreachable").increment(stats.unreachable_nodes as f64);
        gauge!("rustfs_topology_nodes_offline").increment(stats.offline_nodes as f64);
        gauge!("rustfs_topology_nodes_unknown").increment(stats.unknown_nodes as f64);
        gauge!("rustfs_topology_quorum_availability").increment(stats.quorum_availability());
        if let Some(latency) = stats.avg_latency_ms {
            gauge!("rustfs_topology_latency_avg_ms").increment(latency as f64);
        }
        histogram!("rustfs_topology_avg_error_rate").record(stats.avg_error_rate);

        ClusterMetricsSnapshot {
            captured_at: Utc::now(),
            stats,
            top_nodes,
        }
    }
}

impl Drop for MetricsCollector {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TopologyConfig;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_metrics_collector_streams_snapshots() {
        let config = TopologyConfig::default();
        let topology = Arc::new(SystemTopology::new("collector-test", config).await.unwrap());

        topology
            .register_node("node1".to_string(), "localhost:9001".to_string())
            .await;
        topology.record_node_operation("node1", true, Some(5)).await.unwrap();

        let mut collector = MetricsCollector::new(topology.clone(), 1).await;
        collector.start();

        let mut rx = collector.subscribe();
        timeout(Duration::from_secs(3), rx.changed())
            .await
            .expect("collector did not emit snapshot")
            .unwrap();

        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.stats.total_nodes, 1);
        assert!(!snapshot.top_nodes.is_empty());

        collector.stop();
    }
}
