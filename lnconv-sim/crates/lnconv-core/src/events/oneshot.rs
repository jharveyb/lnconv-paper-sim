//! Static one-shot event generators. No randomness; the same input
//! always produces the same event list.

use std::time::Duration;

use super::EventSchedule;
use crate::channels::ChannelRegistry;
use crate::message::{Gossip, GossipKind, NodeId};

/// Every node emits one fresh message at `at`, on the first
/// `(scid, direction)` it owns. Nodes that own no channels (possible
/// only when `num_scids` is small relative to `num_nodes`) are silently
/// skipped. Useful for stress-testing "how does the protocol cope when
/// N gossips need to spread at once".
pub struct OneShotAll {
    pub at: Duration,
    pub size_bytes: u32,
}

impl EventSchedule for OneShotAll {
    fn build(
        &self,
        num_nodes: usize,
        _max: Duration,
        registry: &ChannelRegistry,
    ) -> Vec<(Duration, NodeId, Gossip)> {
        let mut out = Vec::new();
        let mut next_id: u32 = 0;
        for node in 0..num_nodes as u32 {
            let owned = registry.channels_for(node);
            let Some(&(scid, direction)) = owned.first() else {
                eprintln!(
                    "OneShotAll: skipping node {node} — owns no channels (try a larger [channels].count)"
                );
                continue;
            };
            let msg = Gossip {
                id: next_id,
                origin: node,
                kind: GossipKind::Full,
                size_bytes: self.size_bytes,
                scid,
                direction,
                timestamp: 0,
            };
            out.push((self.at, node, msg));
            next_id += 1;
        }
        out
    }
}

/// A single node emits one message at `at`. Picks the first
/// `(scid, direction)` that node owns; warns and emits nothing if the
/// node owns no channels.
pub struct OneShotSingle {
    pub node: NodeId,
    pub at: Duration,
    pub size_bytes: u32,
}

impl EventSchedule for OneShotSingle {
    fn build(
        &self,
        _num_nodes: usize,
        _max: Duration,
        registry: &ChannelRegistry,
    ) -> Vec<(Duration, NodeId, Gossip)> {
        let owned = registry.channels_for(self.node);
        let Some(&(scid, direction)) = owned.first() else {
            eprintln!(
                "OneShotSingle: node {} owns no channels (try a larger [channels].count)",
                self.node
            );
            return Vec::new();
        };
        let msg = Gossip {
            id: 0,
            origin: self.node,
            kind: GossipKind::Full,
            size_bytes: self.size_bytes,
            scid,
            direction,
            timestamp: 0,
        };
        vec![(self.at, self.node, msg)]
    }
}
