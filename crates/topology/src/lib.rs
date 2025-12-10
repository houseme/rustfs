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

//! # RustFS Topology Management
//!
//! Global topology and node state management for RustFS distributed cluster.
//! Provides centralized tracking of node health, metrics, and cluster membership.
//!
//! ## Features
//!
//! - **Asynchronous State Management**: Background health monitoring with sub-second detection
//! - **Node Health Tracking**: Multi-state health model (Healthy/Degraded/Unreachable/Offline/Suspended)
//! - **Performance Metrics**: Real-time latency, error rate, and request statistics
//! - **Cluster Membership**: Dynamic node registration and discovery
//!
//! ## Example
//!
//! ```rust,no_run
//! use rustfs_topology::{SystemTopology, TopologyConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = TopologyConfig::default();
//!     let topology = SystemTopology::new("cluster-1", config).await?;
//!     
//!     // Monitor cluster health
//!     let healthy_nodes = topology.get_healthy_nodes().await;
//!     println!("Healthy nodes: {}", healthy_nodes.len());
//!     
//!     Ok(())
//! }
//! ```

pub mod health_monitor;
pub mod metrics_collector;
pub mod topology;
pub mod types;

pub use health_monitor::HealthMonitor;
pub use topology::SystemTopology;
pub use types::*;

/// Re-export commonly used types
pub use types::{NodeHealth, NodeStatus, TopologyConfig};
