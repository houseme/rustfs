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

//! Quorum-related error types

use thiserror::Error;

/// Errors related to quorum operations
#[derive(Debug, Error)]
pub enum QuorumError {
    /// Insufficient nodes available for write quorum
    #[error("Insufficient write quorum: required {required}, available {available}")]
    InsufficientWriteQuorum { required: usize, available: usize },

    /// Insufficient nodes available for read quorum
    #[error("Insufficient read quorum: required {required}, available {available}")]
    InsufficientReadQuorum { required: usize, available: usize },

    /// Node is not part of the quorum set
    #[error("Node {node_id} is not in quorum set")]
    NodeNotInQuorum { node_id: String },

    /// Quorum configuration is invalid
    #[error("Invalid quorum configuration: {reason}")]
    InvalidConfiguration { reason: String },
}
