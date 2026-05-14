//! Channel ownership registry.
//!
//! Each `(scid, direction)` pair belongs to exactly one node — its
//! "owner". Built once at sim init from either a synthetic random
//! sample or a real LN snapshot loaded from CSV. Internally maps
//! `(scid, direction) -> NodeId` and `NodeId -> Vec<(scid, direction)>`
//! via `HashMap`s — needed because both `Scid` and `NodeId` are u64
//! and may be sparse hashes (CSV-loaded) rather than dense 0..n.
//!
//! Both constructors populate the topology's `channels` directed graph
//! at the same time so the graph view and the registry view stay in
//! sync. The registry also caches a `Vec<Scid>` of all SCIDs in
//! insertion order so event-stream generators can sample uniformly
//! by index.
//!
//! The registry is read-only after construction. It's consumed by the
//! event-stream generators (to look up the originator for each
//! `(scid, direction)` they sample) and at startup logging.
//!
//! Per-node state on the simulation models doesn't reference this — node
//! `lngraph`s key on whatever `(scid, direction)` they actually receive.

use std::collections::HashMap;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::message::{Direction, NodeId, Scid};
use crate::topology::Topology;

pub struct ChannelRegistry {
    /// Total channel count. May differ from `scids.len()` only if a
    /// future caller passes duplicate SCIDs (which should error
    /// upstream — we treat it as the real count of unique entries).
    pub num_scids: usize,
    /// SCIDs in insertion order. Used by `random_channel` and by the
    /// CSV cross-check tests.
    scids: Vec<Scid>,
    /// `(scid, dir) -> owner NodeId`. Sparse-key-friendly.
    owners: HashMap<(Scid, Direction), NodeId>,
    /// `node -> list of (scid, direction)` it owns. Sparse-key-friendly.
    per_node: HashMap<NodeId, Vec<(Scid, Direction)>>,
}

impl ChannelRegistry {
    /// Build the registry from any iterator of `(scid, dir0_owner,
    /// dir1_owner)` tuples and populate the topology's `channels` graph
    /// in the same pass. Used by both the synthetic random builder and
    /// the CSV loader.
    pub fn from_iter<I>(topology: &mut Topology, channels: I) -> Self
    where
        I: IntoIterator<Item = (Scid, NodeId, NodeId)>,
    {
        let mut scids: Vec<Scid> = Vec::new();
        let mut owners: HashMap<(Scid, Direction), NodeId> = HashMap::new();
        let mut per_node: HashMap<NodeId, Vec<(Scid, Direction)>> = HashMap::new();
        for (scid, d0, d1) in channels {
            assert_ne!(d0, d1, "channel endpoints must be distinct");
            scids.push(scid);
            owners.insert((scid, 0), d0);
            owners.insert((scid, 1), d1);
            per_node.entry(d0).or_default().push((scid, 0));
            per_node.entry(d1).or_default().push((scid, 1));
            topology.add_channel(scid, d0, d1);
        }
        Self {
            num_scids: scids.len(),
            scids,
            owners,
            per_node,
        }
    }

    /// Synthetic builder: sample `num_scids` channels at random. SCIDs
    /// are dense `0..num_scids` cast to `Scid` (u64), so existing
    /// configs preserve their channel-id space exactly.
    pub fn build(topology: &mut Topology, num_scids: u32, seed: u64) -> Self {
        let num_nodes = topology.len();
        assert!(num_nodes >= 2, "need at least two nodes to form a channel");
        let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xC4A);
        let n_u64 = num_nodes as u64;
        let tuples = (0..num_scids).map(|scid| {
            let a = rng.random_range(0..n_u64);
            let mut b = rng.random_range(0..n_u64);
            while b == a {
                b = rng.random_range(0..n_u64);
            }
            // Coin-flip which side gets direction 0.
            let (d0, d1) = if rng.random::<bool>() { (a, b) } else { (b, a) };
            (scid as Scid, d0 as NodeId, d1 as NodeId)
        }).collect::<Vec<_>>();
        Self::from_iter(topology, tuples)
    }

    pub fn owner(&self, scid: Scid, direction: Direction) -> NodeId {
        *self
            .owners
            .get(&(scid, direction))
            .expect("unknown (scid, direction)")
    }

    /// Cheap "do we know this SCID at all" check. The parquet replay
    /// loader uses this to skip events that reference channels not
    /// present in the snapshot — calling `owner()` on an unknown SCID
    /// would panic.
    pub fn knows_scid(&self, scid: Scid) -> bool {
        self.owners.contains_key(&(scid, 0))
    }

    pub fn channels_for(&self, node: NodeId) -> &[(Scid, Direction)] {
        self.per_node
            .get(&node)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn len(&self) -> usize {
        self.num_scids
    }

    pub fn is_empty(&self) -> bool {
        self.num_scids == 0
    }

    /// Pick a random `(scid, direction, owner)` triple uniformly from
    /// the registry. Used by `PoissonRandom` to drive its event
    /// originations.
    pub fn random_channel(&self, rng: &mut ChaCha8Rng) -> (Scid, Direction, NodeId) {
        debug_assert!(!self.scids.is_empty(), "registry is empty");
        let idx = rng.random_range(0..self.scids.len());
        let scid = self.scids[idx];
        let direction: Direction = if rng.random::<bool>() { 1 } else { 0 };
        let owner = self.owner(scid, direction);
        (scid, direction, owner)
    }

    /// All SCIDs in insertion order. Used by reshape passes (e.g.
    /// `parquet_replay`) that need to enumerate every channel for
    /// capacity tracking.
    pub fn scids(&self) -> &[Scid] {
        &self.scids
    }

    /// Mean number of `(scid, direction)` pairs owned by each node in
    /// the topology. Equals `2 * num_scids / num_nodes` (each scid has
    /// two directions, each owned by one node).
    pub fn mean_per_node(&self) -> f64 {
        if self.per_node.is_empty() {
            return 0.0;
        }
        let total: usize = self.per_node.values().map(|v| v.len()).sum();
        total as f64 / self.per_node.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{NodeAlgo, Topology};

    fn topo(n: usize) -> Topology {
        Topology::empty(n, NodeAlgo::Flooding)
    }

    #[test]
    fn build_is_deterministic() {
        let mut t1 = topo(50);
        let mut t2 = topo(50);
        let a = ChannelRegistry::build(&mut t1, 100, 42);
        let b = ChannelRegistry::build(&mut t2, 100, 42);
        for scid in 0..100u64 {
            assert_eq!(a.owner(scid, 0), b.owner(scid, 0));
            assert_eq!(a.owner(scid, 1), b.owner(scid, 1));
        }
    }

    #[test]
    fn owner_matches_channels_for() {
        let mut t = topo(30);
        let r = ChannelRegistry::build(&mut t, 200, 7);
        for scid in 0..200u64 {
            for dir in [0u8, 1u8] {
                let owner = r.owner(scid, dir);
                assert!(r.channels_for(owner).contains(&(scid, dir)));
            }
        }
    }

    #[test]
    fn directions_are_distinct_nodes() {
        let mut t = topo(10);
        let r = ChannelRegistry::build(&mut t, 50, 1);
        for scid in 0..50u64 {
            assert_ne!(r.owner(scid, 0), r.owner(scid, 1));
        }
    }

    /// Cross-check: every (scid, direction) the registry says is owned
    /// by node N must also be reachable from N's vertex in the topology
    /// channels graph.
    #[test]
    fn topology_channels_match_registry() {
        let mut t = topo(20);
        let r = ChannelRegistry::build(&mut t, 80, 99);
        for node in 0..20u64 {
            let from_registry: std::collections::HashSet<_> =
                r.channels_for(node).iter().copied().collect();
            let from_graph: std::collections::HashSet<_> = t.channels_for(node).collect();
            assert_eq!(from_registry, from_graph, "mismatch for node {node}");
        }
    }
}
