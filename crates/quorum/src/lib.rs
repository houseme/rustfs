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

//! # RustFS Quorum Management
//!
//! Quorum and consensus mechanism for distributed operations.
//! Implements write and read quorum rules for cluster operations.
//!
//! ## Quorum Rules
//!
//! - **Write Quorum**: N/2 + 1 (majority of nodes)
//! - **Read Quorum**: N/2 (half of nodes)
//!
//! ## Example
//!
//! ```rust,no_run
//! use rustfs_quorum::QuorumVerifier;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let verifier = QuorumVerifier::new(4); // 4-node cluster
//!     
//!     // Check if write quorum is met
//!     let can_write = verifier.check_write_quorum(3).await?;
//!     assert!(can_write);
//!     
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod verifier;

pub use error::QuorumError;
pub use verifier::QuorumVerifier;
