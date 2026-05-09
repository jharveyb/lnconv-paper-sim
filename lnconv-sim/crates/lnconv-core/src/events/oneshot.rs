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
        nodes: &[NodeId],
        _max: Duration,
        registry: &ChannelRegistry,
    ) -> Vec<(Duration, NodeId, Gossip)> {
        let mut out = Vec::new();
        let mut next_id: u32 = 0;
        for &node in nodes {
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

/// A single node emits one message at `at`. The `node` field is the
/// vertex *index* (0..n) into the topology's NodeId order — for
/// synthetic configs this matches the NodeId itself; for CSV-loaded
/// configs it lets you address a node without knowing its
/// hash-derived NodeId. Picks the first `(scid, direction)` that
/// node owns; warns and emits nothing if the node owns no channels.
pub struct OneShotSingle {
    pub node: usize,
    pub at: Duration,
    pub size_bytes: u32,
}

impl EventSchedule for OneShotSingle {
    fn build(
        &self,
        nodes: &[NodeId],
        _max: Duration,
        registry: &ChannelRegistry,
    ) -> Vec<(Duration, NodeId, Gossip)> {
        let Some(&node_id) = nodes.get(self.node) else {
            eprintln!(
                "OneShotSingle: vertex index {} out of range (have {} nodes)",
                self.node,
                nodes.len()
            );
            return Vec::new();
        };
        let owned = registry.channels_for(node_id);
        let Some(&(scid, direction)) = owned.first() else {
            eprintln!(
                "OneShotSingle: vertex {} (NodeId {}) owns no channels (try a larger [channels].count)",
                self.node, node_id,
            );
            return Vec::new();
        };
        let msg = Gossip {
            id: 0,
            origin: node_id,
            kind: GossipKind::Full,
            size_bytes: self.size_bytes,
            scid,
            direction,
            timestamp: 0,
        };
        vec![(self.at, node_id, msg)]
    }
}
