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

//! Integration tests for cluster manager and topology/quorum architecture
//!
//! These tests validate the end-to-end behavior of the cluster resilience system,
//! including node failure detection, quorum verification, and recovery mechanisms.

use rustfs::cluster_manager::ClusterManager;
use rustfs_topology::{NodeHealth, TopologyConfig};
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Test basic cluster initialization
#[tokio::test]
async fn test_cluster_initialization() {
    let cluster_id = "test-cluster-init";
    let total_nodes = 4;
    let config = TopologyConfig::default();

    let manager = ClusterManager::initialize(cluster_id, total_nodes, config, None)
        .await
        .expect("Failed to initialize cluster manager");

    // Verify cluster ID
    assert_eq!(manager.get_cluster_id(), cluster_id);

    // Verify stats - initially no nodes registered
    let stats = manager.get_cluster_stats().await;
    assert_eq!(stats.total_nodes, 0);
    assert_eq!(stats.healthy_nodes, 0);

    manager.shutdown().await.expect("Failed to shutdown");
}

/// Test node registration and discovery
#[tokio::test]
async fn test_node_registration() {
    let cluster_id = "test-cluster-registration";
    let total_nodes = 4;
    let config = TopologyConfig::default();

    let manager = ClusterManager::initialize(cluster_id, total_nodes, config, None)
        .await
        .expect("Failed to initialize cluster manager");

    // Register nodes
    manager
        .register_node("node1", "192.168.1.1:9000")
        .await
        .expect("Failed to register node1");
    manager
        .register_node("node2", "192.168.1.2:9000")
        .await
        .expect("Failed to register node2");
    manager
        .register_node("node3", "192.168.1.3:9000")
        .await
        .expect("Failed to register node3");

    // Verify registration
    let stats = manager.get_cluster_stats().await;
    assert_eq!(stats.total_nodes, 3);
    // New nodes start as Unknown, not Healthy
    assert_eq!(stats.healthy_nodes, 0);

    manager.shutdown().await.expect("Failed to shutdown");
}

/// Test node health tracking and state transitions
#[tokio::test]
async fn test_node_health_tracking() {
    let cluster_id = "test-cluster-health";
    let total_nodes = 4;
    let config = TopologyConfig::default();

    let manager = ClusterManager::initialize(cluster_id, total_nodes, config, None)
        .await
        .expect("Failed to initialize cluster manager");

    // Register node
    manager
        .register_node("node1", "192.168.1.1:9000")
        .await
        .expect("Failed to register node1");

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
    let node_status = manager.get_node_status("node1").await;
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
    let node_status = manager.get_node_status("node1").await;
    assert!(node_status.is_some());
    let status = node_status.unwrap();
    // After 5 failures, should be degraded or offline
    assert_ne!(status.health, NodeHealth::Healthy);

    manager.shutdown().await.expect("Failed to shutdown");
}

/// Test write quorum verification
#[tokio::test]
async fn test_write_quorum() {
    let cluster_id = "test-cluster-write-quorum";
    let total_nodes = 4; // Write quorum = 3 (N/2 + 1)
    let config = TopologyConfig::default();

    let manager = ClusterManager::initialize(cluster_id, total_nodes, config, None)
        .await
        .expect("Failed to initialize cluster manager");

    // Register nodes
    for i in 1..=4 {
        manager
            .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
            .await
            .expect("Failed to register node");
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

    manager.shutdown().await.expect("Failed to shutdown");
}

/// Test read quorum verification
#[tokio::test]
async fn test_read_quorum() {
    let cluster_id = "test-cluster-read-quorum";
    let total_nodes = 4; // Read quorum = 2 (N/2)
    let config = TopologyConfig::default();

    let manager = ClusterManager::initialize(cluster_id, total_nodes, config, None)
        .await
        .expect("Failed to initialize cluster manager");

    // Register nodes
    for i in 1..=4 {
        manager
            .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
            .await
            .expect("Failed to register node");
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

    manager.shutdown().await.expect("Failed to shutdown");
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

    let manager = ClusterManager::initialize(cluster_id, total_nodes, config, Some(1))
        .await
        .expect("Failed to initialize cluster manager");

    // Register nodes
    for i in 1..=3 {
        manager
            .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
            .await
            .expect("Failed to register node");
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

    manager.shutdown().await.expect("Failed to shutdown");
}

/// Test concurrent node operations
#[tokio::test]
async fn test_concurrent_operations() {
    let cluster_id = "test-cluster-concurrent";
    let total_nodes = 4;
    let config = TopologyConfig::default();

    let manager = Arc::new(
        ClusterManager::initialize(cluster_id, total_nodes, config, None)
            .await
            .expect("Failed to initialize cluster manager"),
    );

    // Register nodes
    for i in 1..=4 {
        manager
            .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
            .await
            .expect("Failed to register node");
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

    manager.shutdown().await.expect("Failed to shutdown");
}

/// Test cluster stats calculation
#[tokio::test]
async fn test_cluster_stats() {
    let cluster_id = "test-cluster-stats";
    let total_nodes = 5;
    let config = TopologyConfig::default();

    let manager = ClusterManager::initialize(cluster_id, total_nodes, config, None)
        .await
        .expect("Failed to initialize cluster manager");

    // Register 5 nodes with different health states
    for i in 1..=5 {
        manager
            .register_node(&format!("node{}", i), &format!("192.168.1.{}:9000", i))
            .await
            .expect("Failed to register node");
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
    assert!(stats.has_write_quorum); // 3 >= (5/2 + 1) = 3
    assert!(stats.has_read_quorum); // 3 >= (5/2) = 2

    manager.shutdown().await.expect("Failed to shutdown");
}
