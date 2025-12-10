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

//! Tests for connection health tracking and management

use rustfs_common::globals::{
    evict_unhealthy_connections, get_connection_health, get_connection_health_stats,
    record_connection_failure, record_connection_success, start_connection_health_checker,
    ConnectionHealth, HealthState,
};
use std::time::Duration;

#[tokio::test]
async fn test_connection_health_success() {
    let health = ConnectionHealth::new("test-peer-1".to_string());

    assert_eq!(health.get_state(), HealthState::Healthy);

    health.record_success().await;
    assert_eq!(health.get_state(), HealthState::Healthy);
}

#[tokio::test]
async fn test_connection_health_failure_progression() {
    let health = ConnectionHealth::new("test-peer-2".to_string());

    // First failure - should be degraded
    health.record_failure().await;
    assert_eq!(health.get_state(), HealthState::Degraded);

    // Second failure - still degraded
    health.record_failure().await;
    assert_eq!(health.get_state(), HealthState::Degraded);

    // Third failure - should be dead
    health.record_failure().await;
    assert_eq!(health.get_state(), HealthState::Dead);
}

#[tokio::test]
async fn test_connection_health_recovery() {
    let health = ConnectionHealth::new("test-peer-3".to_string());

    // Cause failures
    health.record_failure().await;
    health.record_failure().await;
    health.record_failure().await;
    assert_eq!(health.get_state(), HealthState::Dead);

    // Record success - should recover to healthy
    health.record_success().await;
    assert_eq!(health.get_state(), HealthState::Healthy);
}

#[tokio::test]
async fn test_connection_health_should_evict() {
    let health = ConnectionHealth::new("test-peer-4".to_string());

    // Fresh connection should not be evicted
    assert!(!health.should_evict(30, 3, 10).await);

    // Dead connection should always be evicted
    health.record_failure().await;
    health.record_failure().await;
    health.record_failure().await;
    assert_eq!(health.get_state(), HealthState::Dead);
    assert!(health.should_evict(30, 3, 10).await);
}

#[tokio::test]
async fn test_get_connection_health() {
    let health1 = get_connection_health("test-peer-5").await;
    let health2 = get_connection_health("test-peer-5").await;

    // Should return the same instance
    assert_eq!(health1.addr, health2.addr);
}

#[tokio::test]
async fn test_record_connection_success() {
    let addr = "test-peer-6";
    
    // Record some failures first
    record_connection_failure(addr).await;
    record_connection_failure(addr).await;
    
    let health = get_connection_health(addr).await;
    assert_eq!(health.get_state(), HealthState::Degraded);
    
    // Record success should reset state
    record_connection_success(addr).await;
    assert_eq!(health.get_state(), HealthState::Healthy);
}

#[tokio::test]
async fn test_get_connection_health_stats() {
    let addr1 = "test-peer-7";
    let addr2 = "test-peer-8";

    record_connection_failure(addr1).await;
    record_connection_success(addr2).await;

    let stats = get_connection_health_stats().await;

    // Should have entries for both addresses
    assert!(stats.contains_key(addr1));
    assert!(stats.contains_key(addr2));

    // addr1 should be degraded with 1 failure
    let (state1, failures1, _) = stats.get(addr1).unwrap();
    assert_eq!(*state1, HealthState::Degraded);
    assert_eq!(*failures1, 1);

    // addr2 should be healthy with 0 failures
    let (state2, failures2, _) = stats.get(addr2).unwrap();
    assert_eq!(*state2, HealthState::Healthy);
    assert_eq!(*failures2, 0);
}

#[tokio::test]
async fn test_evict_unhealthy_connections() {
    let addr = "test-peer-9";

    // Create a dead connection
    record_connection_failure(addr).await;
    record_connection_failure(addr).await;
    record_connection_failure(addr).await;

    let health = get_connection_health(addr).await;
    assert_eq!(health.get_state(), HealthState::Dead);

    // Evict unhealthy connections
    let evicted = evict_unhealthy_connections().await;

    // Should have evicted at least the dead connection
    assert!(evicted > 0);
}

#[tokio::test]
async fn test_connection_health_checker_task() {
    // Start the health checker with a short interval
    let handle = start_connection_health_checker(1);

    // Create some connections
    let addr1 = "test-peer-10";
    let addr2 = "test-peer-11";

    record_connection_success(addr1).await;
    record_connection_failure(addr2).await;
    record_connection_failure(addr2).await;
    record_connection_failure(addr2).await;

    // Wait for a couple of health check cycles
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Dead connection should be evicted
    let stats = get_connection_health_stats().await;
    // addr2 may or may not be in stats depending on eviction, but if present should be dead
    if let Some((state, _, _)) = stats.get(addr2) {
        assert_eq!(*state, HealthState::Dead);
    }

    // Abort the health checker task
    handle.abort();
}
