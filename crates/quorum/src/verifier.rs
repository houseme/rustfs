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

//! Quorum verification logic

use crate::error::QuorumError;
use anyhow::Result;

/// Quorum verifier for cluster operations
#[derive(Debug, Clone)]
pub struct QuorumVerifier {
    #[allow(dead_code)]
    total_nodes: usize,
    write_quorum: usize,
    read_quorum: usize,
}

impl QuorumVerifier {
    /// Create a new quorum verifier
    pub fn new(total_nodes: usize) -> Self {
        let write_quorum = total_nodes / 2 + 1;
        let read_quorum = total_nodes / 2;

        Self {
            total_nodes,
            write_quorum,
            read_quorum,
        }
    }

    /// Check if write quorum is met
    pub async fn check_write_quorum(&self, available_nodes: usize) -> Result<bool> {
        Ok(available_nodes >= self.write_quorum)
    }

    /// Check if read quorum is met
    pub async fn check_read_quorum(&self, available_nodes: usize) -> Result<bool> {
        Ok(available_nodes >= self.read_quorum)
    }

    /// Verify write quorum or return error
    pub async fn verify_write_quorum(&self, available_nodes: usize) -> Result<(), QuorumError> {
        if available_nodes >= self.write_quorum {
            Ok(())
        } else {
            Err(QuorumError::InsufficientWriteQuorum {
                required: self.write_quorum,
                available: available_nodes,
            })
        }
    }

    /// Verify read quorum or return error
    pub async fn verify_read_quorum(&self, available_nodes: usize) -> Result<(), QuorumError> {
        if available_nodes >= self.read_quorum {
            Ok(())
        } else {
            Err(QuorumError::InsufficientReadQuorum {
                required: self.read_quorum,
                available: available_nodes,
            })
        }
    }

    /// Get write quorum size
    pub fn write_quorum_size(&self) -> usize {
        self.write_quorum
    }

    /// Get read quorum size
    pub fn read_quorum_size(&self) -> usize {
        self.read_quorum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_write_quorum() {
        let verifier = QuorumVerifier::new(4);
        assert_eq!(verifier.write_quorum_size(), 3);

        assert!(verifier.check_write_quorum(3).await.unwrap());
        assert!(verifier.check_write_quorum(4).await.unwrap());
        assert!(!verifier.check_write_quorum(2).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_quorum() {
        let verifier = QuorumVerifier::new(4);
        assert_eq!(verifier.read_quorum_size(), 2);

        assert!(verifier.check_read_quorum(2).await.unwrap());
        assert!(verifier.check_read_quorum(3).await.unwrap());
        assert!(!verifier.check_read_quorum(1).await.unwrap());
    }
}
