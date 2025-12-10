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

//! EC (Erasure Coding) Quorum Management System
//!
//! Provides automatic disk elimination and write operation validation based on
//! quorum requirements. Integrates with circuit breaker for fast failure detection.

use rustfs_common::globals::{record_peer_failure, record_peer_success, should_attempt_peer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// EC quorum configuration
#[derive(Debug, Clone)]
pub struct QuorumConfig {
    /// Number of data shards (D)
    pub data_shards: usize,
    /// Number of parity shards (P)
    pub parity_shards: usize,
    /// Disk health check timeout in seconds
    pub disk_health_timeout_secs: u64,
}

impl QuorumConfig {
    /// Total number of disks (N = D + P)
    pub fn total_disks(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Write quorum requirement (D + 1)
    pub fn write_quorum(&self) -> usize {
        self.data_shards + 1
    }

    /// Read quorum requirement (D)
    pub fn read_quorum(&self) -> usize {
        self.data_shards
    }
}

impl Default for QuorumConfig {
    fn default() -> Self {
        Self {
            data_shards: 4,
            parity_shards: 2,
            disk_health_timeout_secs: 3,
        }
    }
}

/// Disk health state for quorum management
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskHealthState {
    /// Disk is online and healthy
    Online,
    /// Disk is temporarily offline (circuit breaker open)
    Offline,
    /// Disk is being checked
    Checking,
}

/// Tracks health of a single disk
pub struct DiskHealth {
    /// Disk identifier (address or path)
    pub disk_id: String,
    /// Current health state
    pub state: RwLock<DiskHealthState>,
    /// Last successful operation timestamp
    pub last_success: RwLock<SystemTime>,
    /// Number of consecutive failures
    pub consecutive_failures: AtomicUsize,
}

impl DiskHealth {
    pub fn new(disk_id: String) -> Self {
        Self {
            disk_id,
            state: RwLock::new(DiskHealthState::Online),
            last_success: RwLock::new(SystemTime::now()),
            consecutive_failures: AtomicUsize::new(0),
        }
    }

    /// Record successful operation
    pub async fn record_success(&self) {
        *self.last_success.write().await = SystemTime::now();
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let mut state = self.state.write().await;
        if *state != DiskHealthState::Online {
            info!("Disk {} recovered to online", self.disk_id);
            *state = DiskHealthState::Online;
        }
    }

    /// Record failed operation
    pub async fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= 3 {
            let mut state = self.state.write().await;
            if *state == DiskHealthState::Online {
                warn!("Disk {} marked offline after {} failures", self.disk_id, failures);
                *state = DiskHealthState::Offline;
            }
        }
    }

    /// Check if disk is available for operations
    pub async fn is_available(&self) -> bool {
        matches!(*self.state.read().await, DiskHealthState::Online)
    }
}

/// Quorum manager for EC operations
pub struct QuorumManager {
    /// Configuration
    pub config: QuorumConfig,
    /// Disk health tracking
    pub disks: Vec<Arc<DiskHealth>>,
    /// Write operations paused flag
    pub write_paused: AtomicBool,
    /// Last quorum check timestamp
    pub last_quorum_check: RwLock<SystemTime>,
    /// Metrics: total write operations
    pub metrics_write_ops: AtomicU64,
    /// Metrics: write operations blocked due to quorum
    pub metrics_write_blocked: AtomicU64,
    /// Metrics: disks eliminated
    pub metrics_disks_eliminated: AtomicU64,
}

impl QuorumManager {
    /// Create a new quorum manager
    pub fn new(config: QuorumConfig, disk_ids: Vec<String>) -> Arc<Self> {
        let disks = disk_ids.into_iter().map(|id| Arc::new(DiskHealth::new(id))).collect();

        Arc::new(Self {
            config,
            disks,
            write_paused: AtomicBool::new(false),
            last_quorum_check: RwLock::new(SystemTime::now()),
            metrics_write_ops: AtomicU64::new(0),
            metrics_write_blocked: AtomicU64::new(0),
            metrics_disks_eliminated: AtomicU64::new(0),
        })
    }

    /// Verify write quorum before operation
    ///
    /// Returns Ok(available_disks) if quorum is met, Err otherwise
    pub async fn verify_write_quorum(&self) -> Result<Vec<Arc<DiskHealth>>, QuorumError> {
        self.metrics_write_ops.fetch_add(1, Ordering::Relaxed);

        // Update quorum check timestamp
        *self.last_quorum_check.write().await = SystemTime::now();

        // Collect available disks
        let mut available = Vec::new();
        for disk in &self.disks {
            // Check both disk health and circuit breaker state
            if disk.is_available().await && should_attempt_peer(&disk.disk_id).await {
                available.push(disk.clone());
            } else if disk.is_available().await {
                debug!("Disk {} skipped due to circuit breaker", disk.disk_id);
            }
        }

        let available_count = available.len();
        let required = self.config.write_quorum();

        if available_count >= required {
            debug!(
                "Write quorum verified: {}/{} disks available (required: {})",
                available_count,
                self.disks.len(),
                required
            );
            Ok(available)
        } else {
            self.metrics_write_blocked.fetch_add(1, Ordering::Relaxed);
            warn!(
                "Write quorum NOT met: only {}/{} disks available (required: {})",
                available_count,
                self.disks.len(),
                required
            );
            Err(QuorumError::InsufficientDisks {
                available: available_count,
                required,
            })
        }
    }

    /// Verify read quorum
    pub async fn verify_read_quorum(&self) -> Result<Vec<Arc<DiskHealth>>, QuorumError> {
        let mut available = Vec::new();
        for disk in &self.disks {
            if disk.is_available().await && should_attempt_peer(&disk.disk_id).await {
                available.push(disk.clone());
            }
        }

        let available_count = available.len();
        let required = self.config.read_quorum();

        if available_count >= required {
            Ok(available)
        } else {
            Err(QuorumError::InsufficientDisks {
                available: available_count,
                required,
            })
        }
    }

    /// Record operation result for a disk
    pub async fn record_disk_result(&self, disk_id: &str, success: bool) {
        if let Some(disk) = self.disks.iter().find(|d| d.disk_id == disk_id) {
            if success {
                disk.record_success().await;
                record_peer_success(disk_id).await;
            } else {
                disk.record_failure().await;
                record_peer_failure(disk_id).await;
            }
        }

        // Check if we need to pause writes
        self.update_write_pause_state().await;
    }

    /// Update write pause state based on available disks
    async fn update_write_pause_state(&self) {
        let available_count = self.count_available_disks().await;
        let required = self.config.write_quorum();

        let should_pause = available_count < required;
        let was_paused = self.write_paused.swap(should_pause, Ordering::Relaxed);

        if should_pause && !was_paused {
            warn!(
                "WRITE OPERATIONS PAUSED: only {}/{} disks available (required: {})",
                available_count,
                self.disks.len(),
                required
            );
        } else if !should_pause && was_paused {
            info!("WRITE OPERATIONS RESUMED: {}/{} disks now available", available_count, self.disks.len());
        }
    }

    /// Count currently available disks
    async fn count_available_disks(&self) -> usize {
        let mut count = 0;
        for disk in &self.disks {
            if disk.is_available().await && should_attempt_peer(&disk.disk_id).await {
                count += 1;
            }
        }
        count
    }

    /// Get quorum statistics
    pub async fn get_stats(&self) -> QuorumStats {
        let mut online = 0;
        let mut offline = 0;
        let mut checking = 0;

        for disk in &self.disks {
            match *disk.state.read().await {
                DiskHealthState::Online => online += 1,
                DiskHealthState::Offline => offline += 1,
                DiskHealthState::Checking => checking += 1,
            }
        }

        QuorumStats {
            total_disks: self.disks.len(),
            online_disks: online,
            offline_disks: offline,
            checking_disks: checking,
            write_quorum_required: self.config.write_quorum(),
            read_quorum_required: self.config.read_quorum(),
            write_paused: self.write_paused.load(Ordering::Relaxed),
            total_write_ops: self.metrics_write_ops.load(Ordering::Relaxed),
            write_blocked_ops: self.metrics_write_blocked.load(Ordering::Relaxed),
            disks_eliminated: self.metrics_disks_eliminated.load(Ordering::Relaxed),
        }
    }

    /// Eliminate offline disks from write operations
    ///
    /// This is called periodically to update disk states based on circuit breaker status
    pub async fn eliminate_offline_disks(&self) -> usize {
        let mut eliminated = 0;

        for disk in &self.disks {
            // Check circuit breaker state
            if !should_attempt_peer(&disk.disk_id).await {
                let mut state = disk.state.write().await;
                if *state == DiskHealthState::Online {
                    warn!("Eliminating disk {} from operations (circuit breaker open)", disk.disk_id);
                    *state = DiskHealthState::Offline;
                    eliminated += 1;
                }
            }
        }

        if eliminated > 0 {
            self.metrics_disks_eliminated.fetch_add(eliminated as u64, Ordering::Relaxed);
            self.update_write_pause_state().await;
        }

        eliminated
    }
}

/// Quorum error types
#[derive(Debug, Clone)]
pub enum QuorumError {
    /// Insufficient disks available
    InsufficientDisks { available: usize, required: usize },
    /// Write operations are paused
    WritePaused,
    /// Disk not found
    DiskNotFound(String),
}

impl std::fmt::Display for QuorumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuorumError::InsufficientDisks { available, required } => {
                write!(f, "Insufficient disks: {} available, {} required", available, required)
            }
            QuorumError::WritePaused => write!(f, "Write operations are paused due to insufficient disks"),
            QuorumError::DiskNotFound(id) => write!(f, "Disk not found: {}", id),
        }
    }
}

impl std::error::Error for QuorumError {}

/// Quorum statistics
#[derive(Debug, Clone)]
pub struct QuorumStats {
    pub total_disks: usize,
    pub online_disks: usize,
    pub offline_disks: usize,
    pub checking_disks: usize,
    pub write_quorum_required: usize,
    pub read_quorum_required: usize,
    pub write_paused: bool,
    pub total_write_ops: u64,
    pub write_blocked_ops: u64,
    pub disks_eliminated: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_quorum_config() {
        let config = QuorumConfig {
            data_shards: 4,
            parity_shards: 2,
            disk_health_timeout_secs: 3,
        };

        assert_eq!(config.total_disks(), 6);
        assert_eq!(config.write_quorum(), 5); // D + 1
        assert_eq!(config.read_quorum(), 4); // D
    }

    #[tokio::test]
    async fn test_disk_health_state_transition() {
        let disk = DiskHealth::new("disk1".to_string());

        // Initially online
        assert!(disk.is_available().await);

        // Record failures
        disk.record_failure().await;
        disk.record_failure().await;
        assert!(disk.is_available().await); // Still online with 2 failures

        disk.record_failure().await;
        assert!(!disk.is_available().await); // Offline after 3 failures

        // Recovery
        disk.record_success().await;
        assert!(disk.is_available().await); // Back online
        assert_eq!(disk.consecutive_failures.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_quorum_verification() {
        let disk_ids = vec![
            "disk1".to_string(),
            "disk2".to_string(),
            "disk3".to_string(),
            "disk4".to_string(),
            "disk5".to_string(),
            "disk6".to_string(),
        ];

        let config = QuorumConfig {
            data_shards: 4,
            parity_shards: 2,
            disk_health_timeout_secs: 3,
        };

        let manager = QuorumManager::new(config, disk_ids);

        // All disks online - should pass
        let result = manager.verify_write_quorum().await;
        assert!(result.is_ok());

        // Mark 2 disks offline
        manager.disks[0].record_failure().await;
        manager.disks[0].record_failure().await;
        manager.disks[0].record_failure().await;
        manager.disks[1].record_failure().await;
        manager.disks[1].record_failure().await;
        manager.disks[1].record_failure().await;

        // Should still pass (4 online >= 5 required... wait, that's not enough)
        // Actually with D=4, P=2, we need D+1=5 disks, and we have only 4 online
        let result = manager.verify_write_quorum().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_pause_state() {
        let disk_ids = vec![
            "disk1".to_string(),
            "disk2".to_string(),
            "disk3".to_string(),
            "disk4".to_string(),
            "disk5".to_string(),
            "disk6".to_string(),
        ];

        let config = QuorumConfig {
            data_shards: 4,
            parity_shards: 2,
            disk_health_timeout_secs: 3,
        };

        let manager = QuorumManager::new(config, disk_ids);

        // Initially not paused
        assert!(!manager.write_paused.load(Ordering::Relaxed));

        // Mark enough disks offline to trigger pause
        for i in 0..3 {
            manager.record_disk_result(&format!("disk{}", i + 1), false).await;
            manager.record_disk_result(&format!("disk{}", i + 1), false).await;
            manager.record_disk_result(&format!("disk{}", i + 1), false).await;
        }

        // Should be paused now (only 3 online, need 5)
        assert!(manager.write_paused.load(Ordering::Relaxed));
    }
}
