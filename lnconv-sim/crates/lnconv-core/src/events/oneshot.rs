//! Static one-shot event generators. No randomness; the same input
//! always produces the same event list.

use std::time::Duration;

use super::EventSchedule;
use crate::message::{Gossip, GossipKind, NodeId};

/// Every node emits one fresh message at `at`. Useful for stress-testing
/// "how does the protocol cope when N gossips need to spread at once".
pub struct OneShotAll {
    pub at: Duration,
    pub size_bytes: u32,
}

impl EventSchedule for OneShotAll {
    fn build(&self, num_nodes: usize, _max: Duration) -> Vec<(Duration, NodeId, Gossip)> {
        (0..num_nodes as u32)
            .map(|id| {
                let msg = Gossip {
                    id: id as u64,
                    origin: id,
                    kind: GossipKind::Full,
                    size_bytes: self.size_bytes,
                };
                (self.at, id, msg)
            })
            .collect()
    }
}

/// A single node emits one message at `at`. The "trace one wave through
/// the graph" smoke test.
pub struct OneShotSingle {
    pub node: NodeId,
    pub at: Duration,
    pub size_bytes: u32,
}

impl EventSchedule for OneShotSingle {
    fn build(&self, _num_nodes: usize, _max: Duration) -> Vec<(Duration, NodeId, Gossip)> {
        let msg = Gossip {
            id: 0,
            origin: self.node,
            kind: GossipKind::Full,
            size_bytes: self.size_bytes,
        };
        vec![(self.at, self.node, msg)]
    }
}
