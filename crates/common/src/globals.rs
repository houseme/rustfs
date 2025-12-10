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

#![allow(non_upper_case_globals)] // FIXME

use crate::circuit_breaker::CircuitBreakerRegistry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

pub static GLOBAL_LOCAL_NODE_NAME: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_RUSTFS_HOST: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_RUSTFS_PORT: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("9000".to_string()));
pub static GLOBAL_RUSTFS_ADDR: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
/// Health states for a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HealthState {
    /// Connection is healthy
    Healthy = 0,
    /// Connection is degraded (some failures)
    Degraded = 1,
    /// Connection is dead (should be evicted)
    Dead = 2,
}

impl From<u8> for HealthState {
    fn from(val: u8) -> Self {
        match val {
            1 => HealthState::Degraded,
            2 => HealthState::Dead,
            _ => HealthState::Healthy,
        }
    }
}

/// Tracks health metrics for a cached connection
#[derive(Debug)]
pub struct ConnectionHealth {
    /// Address of the peer
    pub addr: String,
    /// Last time the connection was successfully used
    pub last_successful_use: RwLock<SystemTime>,
    /// Number of consecutive failures
    pub consecutive_failures: AtomicU32,
    /// Last time a health check was performed
    pub last_health_check: RwLock<SystemTime>,
    /// Current health state
    pub health_state: AtomicU8,
}

impl ConnectionHealth {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            last_successful_use: RwLock::new(SystemTime::now()),
            consecutive_failures: AtomicU32::new(0),
            last_health_check: RwLock::new(SystemTime::now()),
            health_state: AtomicU8::new(HealthState::Healthy as u8),
        }
    }

    /// Record a successful operation
    pub async fn record_success(&self) {
        *self.last_successful_use.write().await = SystemTime::now();
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.health_state.store(HealthState::Healthy as u8, Ordering::Relaxed);
    }

    /// Record a failed operation
    pub async fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        
        // Mark as degraded after 1 failure, dead after 3 failures
        if failures >= 3 {
            self.health_state.store(HealthState::Dead as u8, Ordering::Relaxed);
        } else if failures >= 1 {
            self.health_state.store(HealthState::Degraded as u8, Ordering::Relaxed);
        }
    }

    /// Update health check timestamp
    pub async fn update_health_check(&self) {
        *self.last_health_check.write().await = SystemTime::now();
    }

    /// Check if connection should be evicted based on health criteria
    pub async fn should_evict(&self, max_idle_secs: u64, max_failures: u32, health_check_interval_secs: u64) -> bool {
        let state = HealthState::from(self.health_state.load(Ordering::Relaxed));
        
        // Always evict dead connections
        if state == HealthState::Dead {
            return true;
        }

        // Evict if not used successfully in the last max_idle_secs
        let last_use = *self.last_successful_use.read().await;
        if let Ok(elapsed) = SystemTime::now().duration_since(last_use) {
            if elapsed.as_secs() > max_idle_secs {
                return true;
            }
        }

        // Evict if too many consecutive failures
        if self.consecutive_failures.load(Ordering::Relaxed) >= max_failures {
            return true;
        }

        // Evict if health check is stale
        let last_check = *self.last_health_check.read().await;
        if let Ok(elapsed) = SystemTime::now().duration_since(last_check) {
            if elapsed.as_secs() > health_check_interval_secs * 3 {
                return true;
            }
        }

        false
    }

    pub fn get_state(&self) -> HealthState {
        HealthState::from(self.health_state.load(Ordering::Relaxed))
    }
}

pub static GLOBAL_CONN_MAP: LazyLock<RwLock<HashMap<String, Channel>>> = LazyLock::new(|| RwLock::new(HashMap::new()));

/// Global connection health tracking map
pub static GLOBAL_CONN_HEALTH: LazyLock<RwLock<HashMap<String, Arc<ConnectionHealth>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Global circuit breaker registry for peer health tracking.
/// Prevents repeated attempts to communicate with dead/unhealthy peers.
pub static GLOBAL_CIRCUIT_BREAKERS: LazyLock<Arc<CircuitBreakerRegistry>> =
    LazyLock::new(|| Arc::new(CircuitBreakerRegistry::new()));

pub async fn set_global_addr(addr: &str) {
    *GLOBAL_RUSTFS_ADDR.write().await = addr.to_string();
}

/// Evict a stale/dead connection from the global connection cache.
/// This is critical for cluster recovery when a node dies unexpectedly (e.g., power-off).
/// By removing the cached connection, subsequent requests will establish a fresh connection.
pub async fn evict_connection(addr: &str) {
    let removed = GLOBAL_CONN_MAP.write().await.remove(addr);
    if removed.is_some() {
        tracing::warn!("Evicted stale connection from cache: {}", addr);
    }
}

/// Check if a connection exists in the cache for the given address.
pub async fn has_cached_connection(addr: &str) -> bool {
    GLOBAL_CONN_MAP.read().await.contains_key(addr)
}

/// Clear all cached connections. Useful for full cluster reset/recovery.
pub async fn clear_all_connections() {
    let mut map = GLOBAL_CONN_MAP.write().await;
    let count = map.len();
    map.clear();
    if count > 0 {
        tracing::warn!("Cleared {} cached connections from global map", count);
    }
}

/// Check if peer should be contacted based on circuit breaker state.
/// Returns true if the peer is healthy or in half-open state (testing recovery).
pub async fn should_attempt_peer(addr: &str) -> bool {
    GLOBAL_CIRCUIT_BREAKERS.should_attempt(addr).await
}

/// Record successful peer communication.
/// Resets failure count and closes circuit breaker if it was open.
pub async fn record_peer_success(addr: &str) {
    GLOBAL_CIRCUIT_BREAKERS.record_success(addr).await;
    
    // Always update connection health (creates tracker if needed)
    let health = get_connection_health(addr).await;
    health.record_success().await;
}

/// Record failed peer communication.
/// Increments failure count and may open circuit breaker after threshold.
pub async fn record_peer_failure(addr: &str) {
    GLOBAL_CIRCUIT_BREAKERS.record_failure(addr).await;
    
    // Always update connection health (creates tracker if needed)
    let health = get_connection_health(addr).await;
    health.record_failure().await;
}

/// Get or create connection health tracker for an address
pub async fn get_connection_health(addr: &str) -> Arc<ConnectionHealth> {
    // Try read lock first
    {
        let health_map = GLOBAL_CONN_HEALTH.read().await;
        if let Some(health) = health_map.get(addr) {
            return health.clone();
        }
    }

    // Need to create new health tracker
    let mut health_map = GLOBAL_CONN_HEALTH.write().await;
    // Double-check in case another task created it
    if let Some(health) = health_map.get(addr) {
        return health.clone();
    }

    let health = Arc::new(ConnectionHealth::new(addr.to_string()));
    health_map.insert(addr.to_string(), health.clone());
    debug!("Created connection health tracker for: {}", addr);
    health
}

/// Record successful connection use (updates both circuit breaker and health tracking)
pub async fn record_connection_success(addr: &str) {
    // record_peer_success already updates connection health, so just call it
    record_peer_success(addr).await;
}

/// Record failed connection use (updates both circuit breaker and health tracking)
pub async fn record_connection_failure(addr: &str) {
    // record_peer_failure already updates connection health, so just call it
    record_peer_failure(addr).await;
}

/// Evict unhealthy connections from the global cache.
/// Returns the number of connections evicted.
///
/// Connections are evicted if they meet any of these criteria:
/// - Not successfully used in the last 30 seconds
/// - 3 or more consecutive failures
/// - Health check stale (3x the health check interval)
pub async fn evict_unhealthy_connections() -> usize {
    const MAX_IDLE_SECS: u64 = 30;
    const MAX_FAILURES: u32 = 3;
    const HEALTH_CHECK_INTERVAL_SECS: u64 = 10;

    let health_map = GLOBAL_CONN_HEALTH.read().await;
    let mut addrs_to_evict = Vec::new();

    for (addr, health) in health_map.iter() {
        if health.should_evict(MAX_IDLE_SECS, MAX_FAILURES, HEALTH_CHECK_INTERVAL_SECS).await {
            addrs_to_evict.push(addr.clone());
        }
    }
    drop(health_map);

    let evicted_count = addrs_to_evict.len();
    if evicted_count > 0 {
        let mut conn_map = GLOBAL_CONN_MAP.write().await;
        let health_map = GLOBAL_CONN_HEALTH.read().await;

        for addr in addrs_to_evict {
            conn_map.remove(&addr);
            if let Some(health) = health_map.get(&addr) {
                warn!(
                    "Evicting unhealthy connection: {} (state={:?}, failures={})",
                    addr,
                    health.get_state(),
                    health.consecutive_failures.load(Ordering::Relaxed)
                );
            }
            // Keep health tracker for metrics, but remove connection
        }
    }

    evicted_count
}

/// Start a background task to periodically check and evict unhealthy connections.
///
/// This task runs every `interval_secs` seconds and evicts connections that:
/// - Haven't been successfully used recently
/// - Have accumulated failures
/// - Have stale health checks
///
/// This is critical for cluster power-off recovery scenarios where cached
/// connections to dead nodes need to be proactively removed.
pub fn start_connection_health_checker(interval_secs: u64) -> tokio::task::JoinHandle<()> {
    info!(
        "Starting connection health checker with interval: {}s",
        interval_secs
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        // Skip the first immediate tick
        interval.tick().await;

        loop {
            interval.tick().await;

            let evicted = evict_unhealthy_connections().await;
            if evicted > 0 {
                info!(
                    "Connection health checker evicted {} unhealthy connections",
                    evicted
                );
            } else {
                debug!("Connection health checker: all connections healthy");
            }

            // Update health check timestamps for all tracked connections
            let health_map = GLOBAL_CONN_HEALTH.read().await;
            for health in health_map.values() {
                health.update_health_check().await;
            }
        }
    })
}

/// Get statistics about all connection health states
pub async fn get_connection_health_stats() -> HashMap<String, (HealthState, u32, u64)> {
    let health_map = GLOBAL_CONN_HEALTH.read().await;
    let mut stats = HashMap::new();

    for (addr, health) in health_map.iter() {
        let state = health.get_state();
        let failures = health.consecutive_failures.load(Ordering::Relaxed);
        let last_use = *health.last_successful_use.read().await;
        let idle_secs = SystemTime::now()
            .duration_since(last_use)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        stats.insert(addr.clone(), (state, failures, idle_secs));
    }

    stats
}
