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

#![allow(dead_code)]

/// Cluster health-related metric descriptors
use crate::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static HEALTH_DRIVES_OFFLINE_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthDrivesOfflineCount,
        "Count of offline drives in the cluster",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_DRIVES_ONLINE_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthDrivesOnlineCount,
        "Count of online drives in the cluster",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_DRIVES_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthDrivesCount,
        "Count of all drives in the cluster",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_CONNECTIONS_CACHED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthConnectionsCachedCount,
        "Count of cached gRPC connections",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_CONNECTIONS_HEALTHY_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthConnectionsHealthyCount,
        "Count of healthy cached connections",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_CONNECTIONS_DEGRADED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthConnectionsDegradedCount,
        "Count of degraded cached connections",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_CONNECTIONS_DEAD_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthConnectionsDeadCount,
        "Count of dead cached connections",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_CONNECTIONS_EVICTED_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::HealthConnectionsEvictedTotal,
        "Total count of evicted connections",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_CIRCUIT_BREAKERS_OPEN_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthCircuitBreakersOpenCount,
        "Count of open circuit breakers",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static HEALTH_CIRCUIT_BREAKERS_HALF_OPEN_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::HealthCircuitBreakersHalfOpenCount,
        "Count of half-open circuit breakers",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static QUORUM_DISKS_ONLINE_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::QuorumDisksOnlineCount,
        "Count of online disks in quorum",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static QUORUM_DISKS_OFFLINE_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::QuorumDisksOfflineCount,
        "Count of offline disks in quorum",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static QUORUM_DISKS_CHECKING_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::QuorumDisksCheckingCount,
        "Count of disks being checked in quorum",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static QUORUM_WRITE_OPS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::QuorumWriteOpsTotal,
        "Total write operations attempted",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static QUORUM_WRITE_BLOCKED_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::QuorumWriteBlockedTotal,
        "Total write operations blocked due to insufficient quorum",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static QUORUM_DISKS_ELIMINATED_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::QuorumDisksEliminatedTotal,
        "Total disks eliminated from operations",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});

pub static QUORUM_WRITE_PAUSED_STATE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::QuorumWritePausedState,
        "Write operations paused state (1=paused, 0=active)",
        &[],
        subsystems::CLUSTER_HEALTH,
    )
});
