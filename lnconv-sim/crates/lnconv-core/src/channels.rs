//! Channel ownership registry.
//!
//! Each `(scid, direction)` pair belongs to exactly one node — its
//! "owner". Built once at sim init: for each SCID we sample two distinct
//! random nodes uniformly, then randomly assign one to `direction = 0`
//! and the other to `direction = 1`. This mirrors how each side of a
//! Lightning channel emits its own `channel_update` for that direction.
//!
//! The registry is read-only after construction. It's consumed by the
//! event-stream generators (to look up the originator for each
//! `(scid, direction)` they sample) and at startup logging.
//!
//! Per-node state on the simulation models doesn't reference this — node
//! `lngraph`s key on whatever `(scid, direction)` they actually receive.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::message::{Direction, NodeId, Scid};

pub struct ChannelRegistry {
    pub num_scids: u32,
    /// `channels[scid as usize] = (owner_for_dir_0, owner_for_dir_1)`.
    channels: Vec<(NodeId, NodeId)>,
    /// `per_node[node] = list of (scid, direction)` this node owns.
    per_node: Vec<Vec<(Scid, Direction)>>,
}

impl ChannelRegistry {
    pub fn build(num_scids: u32, num_nodes: usize, seed: u64) -> Self {
        assert!(num_nodes >= 2, "need at least two nodes to form a channel");
        let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xC4A);
        let mut channels: Vec<(NodeId, NodeId)> = Vec::with_capacity(num_scids as usize);
        let mut per_node: Vec<Vec<(Scid, Direction)>> = vec![Vec::new(); num_nodes];
        let n_u32 = num_nodes as u32;
        for scid in 0..num_scids {
            let a = rng.random_range(0..n_u32);
            let mut b = rng.random_range(0..n_u32);
            while b == a {
                b = rng.random_range(0..n_u32);
            }
            // Coin-flip which side gets direction 0.
            let (d0, d1) = if rng.random::<bool>() { (a, b) } else { (b, a) };
            channels.push((d0, d1));
            per_node[d0 as usize].push((scid, 0));
            per_node[d1 as usize].push((scid, 1));
        }
        Self {
            num_scids,
            channels,
            per_node,
        }
    }

    pub fn owner(&self, scid: Scid, direction: Direction) -> NodeId {
        let (d0, d1) = self.channels[scid as usize];
        if direction == 0 { d0 } else { d1 }
    }

    pub fn channels_for(&self, node: NodeId) -> &[(Scid, Direction)] {
        &self.per_node[node as usize]
    }

    pub fn len(&self) -> usize {
        self.num_scids as usize
    }

    /// Mean number of `(scid, direction)` pairs owned by each node.
    /// Equals `2 * num_scids / num_nodes` exactly (each scid has two
    /// directions, each owned by one node).
    pub fn mean_per_node(&self) -> f64 {
        2.0 * self.num_scids as f64 / self.per_node.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_is_deterministic() {
        let a = ChannelRegistry::build(100, 50, 42);
        let b = ChannelRegistry::build(100, 50, 42);
        for scid in 0..100 {
            assert_eq!(a.owner(scid, 0), b.owner(scid, 0));
            assert_eq!(a.owner(scid, 1), b.owner(scid, 1));
        }
    }

    #[test]
    fn owner_matches_channels_for() {
        let r = ChannelRegistry::build(200, 30, 7);
        for scid in 0..200u32 {
            for dir in [0u8, 1u8] {
                let owner = r.owner(scid, dir);
                assert!(r.channels_for(owner).contains(&(scid, dir)));
            }
        }
    }

    #[test]
    fn directions_are_distinct_nodes() {
        let r = ChannelRegistry::build(50, 10, 1);
        for scid in 0..50u32 {
            assert_ne!(r.owner(scid, 0), r.owner(scid, 1));
        }
    }
}
