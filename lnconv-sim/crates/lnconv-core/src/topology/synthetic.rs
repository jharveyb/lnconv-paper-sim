//! Topology generators.
//!
//! Two paths feed into the same `Topology` shape:
//!
//! * `random_regular(n, k, seed, default_algo)` — synthetic. Builds a
//!   true k-regular peer graph; channels are sampled separately by
//!   `ChannelRegistry::build`.
//! * `build_peer_graph(&mut topology, k, seed, enforce_hub_cap)` — used
//!   after the channel graph has been populated (e.g. from a CSV
//!   snapshot). Adds peer-graph edges per the documented per-node rule:
//!
//!   - `c > max_peer` channel counterparties: keep `max_peer` random
//!     ones as peers.
//!   - `k <= c <= max_peer`: keep all counterparties (no extra strangers).
//!   - `c < k`: keep all counterparties + `(k - c).div_ceil(2)` random
//!     strangers, Halved
//!     (rounded up) so OR-semantics edge union doesn't double mean
//!     degree to ≈ 2k.
//!
//! `enforce_hub_cap` flips whether hubs *block* incoming peer-edges
//! that aren't in their 100-pick set. See `build_peer_graph`'s docs.
//!
//! "Counterparty" here means a distinct neighbour in the *channels*
//! graph (multi-channels between the same two nodes count once).
//! Strangers are sampled uniformly from the full node set excluding
//! self and existing counterparties.

use std::collections::HashSet;

use petgraph::graph::NodeIndex;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rustworkx_core::generators::random_regular_graph;
use rustworkx_core::petgraph::graph::UnGraph as RxUnGraph;
use rustworkx_core::petgraph::visit::EdgeRef;

use super::{NodeAlgo, Topology};
use crate::message::NodeId;

const PEER_BUILD_SUBSEED: u64 = 0x504545525F535542; // "PEER_SUB"

/// True k-regular random graph: every node has exactly `k` neighbours,
/// chosen uniformly at random subject to no self-loops or parallel edges.
/// Built via rustworkx-core's `random_regular_graph` (configuration-model
/// based with rejection).
///
/// For LN-shaped experiments this is the realistic baseline: random
/// regular graphs are small-world (diameter ~`log_k(n)`, ~6-8 hops on
/// 20k nodes with k=6) and so propagation timings come out in the same
/// ballpark as the real network.
pub fn random_regular(n: usize, k: usize, seed: u64, default_algo: NodeAlgo) -> Topology {
    let g: RxUnGraph<(), ()> =
        random_regular_graph(n, k, Some(seed), || (), || ()).expect("random_regular_graph");
    let mut topo = Topology::empty(n, default_algo);
    for edge in g.edge_references() {
        let u = edge.source().index() as NodeId;
        let v = edge.target().index() as NodeId;
        topo.add_peer_edge(u, v);
    }
    topo
}

/// Populate the peer graph from the already-built channel graph using
/// the per-node degree-tier rule described at module top. Deterministic
/// in `seed`.
///
/// `enforce_hub_cap` controls what happens to peer edges *into* a hub
/// (`c > MAX_PEER_COUNTERPARTIES`):
///
/// * **`false`** — Each node's pass only *adds* peer edges incident to
///   itself; it never blocks an edge another node might add. So a hub
///   that picked 100 of its 200 counterparties can still end up with
///   all 200 as peers once the leaves run their own pass. Matches real
///   LN's "leaf always wants to peer with its only channel partner"
///   behavior.
/// * **`true`** — Hubs pre-commit to their 100 picks (deterministic in
///   `seed`), and any peer edge into a hub from a node *not* in that
///   pick set is silently dropped. Hubs end up with exactly 100 peers
///   even though the channel graph gave them more counterparties.
/// 
/// Stats returned by `build_peer_graph` for diagnostics + tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerBuildStats {
    /// Edges blocked in phase 2 by the hub-cap filter (zero when
    /// `enforce_hub_cap = false`). Counts each rejection once per
    /// source — the same `(leaf, hub)` request is counted once even
    /// though both endpoints might have wanted it.
    pub hub_rejected_edges: usize,
    /// Replacement strangers added in phase 3 to compensate for
    /// rejections. Equal to `hub_rejected_edges` whenever the pool of
    /// non-rejecting candidates was large enough (which is the common
    /// case at LN scale).
    pub replacements_added: usize,
}

pub fn build_peer_graph<F, G>(
    topology: &mut Topology,
    k_for: F,
    max_peer_for: G,
    seed: u64,
    enforce_hub_cap: bool,
) -> PeerBuildStats
where
    F: Fn(&NodeAlgo) -> usize,
    G: Fn(&NodeAlgo) -> usize,
{
    let mut stats = PeerBuildStats::default();
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ PEER_BUILD_SUBSEED);
    let n = topology.len();
    if n < 2 {
        return stats;
    }
    let all_indices: Vec<NodeIndex> = topology.peers.node_indices().collect();

    // Phase 1: per-node, decide which counterparties to keep + which
    // strangers to pick. Done in NodeIndex order so RNG draws are
    // deterministic. Hubs' kept set is locked in here so phase 2 can
    // filter incoming-to-hub edges against the *same* picks.
    //
    // Each node's `k` is resolved from its `NodeAlgo` via `k_for`, so
    // CLN/LND/Flooding/Sketch can target different peer-degrees within the
    // same Mix run. Per-node hub cap comes from `max_peer_for` for the
    // same reason — sketch wants a low cap to bound per-peer tickers.
    struct NodePicks {
        kept_counterparties: Vec<NodeIndex>,
        strangers: Vec<NodeIndex>,
        is_hub: bool,
    }
    let mut picks: Vec<NodePicks> = Vec::with_capacity(n);
    for nx in topology.peers.node_indices() {
        let algo = &topology.peers[nx].algo;
        let k = k_for(algo);
        let max_peer = max_peer_for(algo);
        let counterparties: HashSet<NodeIndex> = topology
            .channels
            .neighbors_undirected(nx)
            .filter(|m| *m != nx)
            .collect();
        let counterparties_vec: Vec<NodeIndex> = counterparties.iter().copied().collect();
        let c = counterparties_vec.len();
        let is_hub = c > max_peer;
        let kept_counterparties: Vec<NodeIndex> = if is_hub {
            counterparties_vec
                .choose_multiple(&mut rng, max_peer)
                .copied()
                .collect()
        } else {
            counterparties_vec.clone()
        };

        // Strangers: 0 for hubs, 0 for mid-range (other nodes will
        // connect to us anyway), `(k - c).div_ceil(2)` for sparse.
        let stranger_count = if is_hub {
            0
        } else if c >= k {
            // Leave this branch in. Nodes can make connections to peers that aren't channel
            // counterparties, but here we expect those nodes to connect to us, so we don't
            // need to add them as strangers ourselves.
            0
        } else if c >= k.div_ceil(2) {
            // Halved (round
            // up) so OR-semantics edge union doesn't double mean
            // degree to ≈ 2k. Without inbound strangers these nodes
            // can land slightly below k (e.g. k=4,c=2 → 3 outgoing);
            // that's deemed acceptable because c >= 2 already gives
            // two natural channel-counterparty back-picks.
            (k - c).div_ceil(2)
        } else {
            (k - c - 1)
        };
        let strangers = if stranger_count > 0 {
            let mut excluded: HashSet<NodeIndex> = counterparties;
            excluded.insert(nx);
            pick_strangers(&all_indices, &excluded, stranger_count, &mut rng)
        } else {
            Vec::new()
        };

        picks.push(NodePicks {
            kept_counterparties,
            strangers,
            is_hub,
        });
    }

    // Hub-kept lookup for fast filtering. Indexed by `NodeIndex.index()`;
    // `None` for non-hubs, `Some(set)` for hubs. Empty when the cap is
    // off — we short-circuit the check in that case.
    let hub_kept: Vec<Option<HashSet<NodeIndex>>> = if enforce_hub_cap {
        picks
            .iter()
            .map(|p| {
                if p.is_hub {
                    Some(p.kept_counterparties.iter().copied().collect())
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let edge_allowed = |src: NodeIndex, dst: NodeIndex| -> bool {
        if !enforce_hub_cap {
            return true;
        }
        match &hub_kept[dst.index()] {
            Some(kept) => kept.contains(&src),
            None => true, // dst is not a hub
        }
    };

    // Phase 2: add edges from each node's picks. Filter destinations
    // that are hubs whose kept set excludes the source. Track the
    // number of drops per source so phase 3 can compensate.
    let mut dropped_per_node: Vec<usize> = vec![0; n];
    for (idx, p) in picks.iter().enumerate() {
        let nx = NodeIndex::new(idx);
        for &ny in p.kept_counterparties.iter().chain(p.strangers.iter()) {
            if !edge_allowed(nx, ny) {
                dropped_per_node[idx] += 1;
                stats.hub_rejected_edges += 1;
                continue;
            }
            if !topology.peers.contains_edge(nx, ny) {
                topology.peers.add_edge(nx, ny, ());
            }
        }
    }

    // Phase 3 (only relevant when `enforce_hub_cap = true`): top-up
    // replacement strangers for each node that lost edges in phase 2.
    // Excludes existing peers + any hub that would reject us, so each
    // replacement is guaranteed to land.
    if enforce_hub_cap {
        for (idx, &dropped) in dropped_per_node.iter().enumerate() {
            if dropped == 0 {
                continue;
            }
            let nx = NodeIndex::new(idx);
            let mut excluded: HashSet<NodeIndex> = HashSet::new();
            excluded.insert(nx);
            for ny in topology.peers.neighbors(nx) {
                excluded.insert(ny);
            }
            // Exclude hubs that would reject us — otherwise our
            // replacement would just be dropped again.
            for (i, kept_opt) in hub_kept.iter().enumerate() {
                if let Some(kept) = kept_opt
                    && !kept.contains(&nx)
                {
                    excluded.insert(NodeIndex::new(i));
                }
            }
            for ny in pick_strangers(&all_indices, &excluded, dropped, &mut rng) {
                if !topology.peers.contains_edge(nx, ny) {
                    topology.peers.add_edge(nx, ny, ());
                    stats.replacements_added += 1;
                }
            }
        }
    }

    stats
}

/// Pick up to `count` distinct nodes from `pool` that aren't in
/// `excluded`. Uses rejection sampling — fine because `|excluded|` is
/// always tiny (≤ counterparties ≤ ~100) relative to `pool` (n).
fn pick_strangers(
    pool: &[NodeIndex],
    excluded: &HashSet<NodeIndex>,
    count: usize,
    rng: &mut ChaCha8Rng,
) -> Vec<NodeIndex> {
    let available = pool.len().saturating_sub(excluded.len());
    let target = count.min(available);
    let mut picked: HashSet<NodeIndex> = HashSet::with_capacity(target);
    let pool_len = pool.len();
    while picked.len() < target {
        let idx = rng.random_range(0..pool_len);
        let candidate = pool[idx];
        if excluded.contains(&candidate) || picked.contains(&candidate) {
            continue;
        }
        picked.insert(candidate);
    }
    picked.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Topology;

    fn topo_with_channels(n: usize, channels: &[(NodeId, NodeId)]) -> Topology {
        let mut t = Topology::empty(n, NodeAlgo::Flooding);
        for (i, (a, b)) in channels.iter().enumerate() {
            t.add_channel(i as u64, *a, *b);
        }
        t
    }

    /// Test helper: closure that returns the same `k` for any algo.
    fn const_k(k: usize) -> impl Fn(&NodeAlgo) -> usize {
        move |_| k
    }

    /// Test helper: closure that returns the same hub-cap for any
    /// algo. Default test cap is 100, matching the production stagger
    /// default.
    fn const_max_peer(c: usize) -> impl Fn(&NodeAlgo) -> usize {
        move |_| c
    }

    /// With `enforce_hub_cap = false` (default OR semantics), a hub's
    /// own pass only ever picks 100 counterparties — but leaf nodes
    /// will still pick the hub back, so the hub's *final* peer count
    /// can exceed 100.
    #[test]
    fn hub_peer_count_or_semantics() {
        let chans: Vec<(NodeId, NodeId)> = (1..=200u64).map(|i| (0u64, i)).collect();
        let mut t = topo_with_channels(220, &chans);
        build_peer_graph(&mut t, const_k(5), const_max_peer(100), 42, false);
        let nx0 = t.nidx(0);
        let peer_count = t.peers.neighbors(nx0).count();
        // At least 100 (its own pick); at most 200 + strangers other nodes
        // happened to point at it (~few in expectation for n=220).
        assert!(peer_count >= 100, "got {peer_count}, expected >= 100");
        assert!(peer_count <= 220, "got {peer_count}, expected <= 220");
    }

    /// With `enforce_hub_cap = true`, a hub commits to 100 picks and
    /// rejects all other incoming peer-edge requests, ending with
    /// exactly 100 peers. Same channel structure as `or_semantics`,
    /// same seed — the only knob that changed is the cap.
    #[test]
    fn hub_peer_count_capped_at_100() {
        let chans: Vec<(NodeId, NodeId)> = (1..=200u64).map(|i| (0u64, i)).collect();
        let mut t = topo_with_channels(220, &chans);
        build_peer_graph(&mut t, const_k(5), const_max_peer(100), 42, true);
        let nx0 = t.nidx(0);
        let peer_count = t.peers.neighbors(nx0).count();
        assert_eq!(
            peer_count, 100,
            "hub with c=200, enforce_hub_cap=true should keep exactly 100 peers; got {peer_count}"
        );
        // The 100 peers are a subset of node 0's channel counterparties.
        for ny in t.peers.neighbors(nx0) {
            let id = t.peers[ny].id;
            assert!(
                (1..=200).contains(&id),
                "peer {id} is not a channel counterparty of node 0"
            );
        }
    }

    /// With the cap on, every dropped peer-edge from phase 2 should
    /// be matched by a replacement stranger in phase 3. Asserts via
    /// the returned `PeerBuildStats` so the test isn't muddled by
    /// other-node OR contributions.
    #[test]
    fn hub_cap_top_up_replaces_dropped_edges() {
        // 200 channels into a single hub. Cap forces it to keep 100.
        // Each of the other 100 leaves wants to peer with the hub,
        // gets rejected, and should get a replacement stranger.
        let chans: Vec<(NodeId, NodeId)> = (1..=200u64).map(|i| (0u64, i)).collect();
        let mut t = topo_with_channels(2000, &chans);
        let stats = build_peer_graph(&mut t, const_k(4), const_max_peer(100), 7, true);
        // At least 100 rejections — that's the leaf counterparty side.
        // Strangers from sparse nodes may also occasionally pick the
        // hub and get rejected, so the total can be slightly higher.
        assert!(
            stats.hub_rejected_edges >= 100,
            "expected ≥ 100 hub rejections (100 leaves + occasional \
             stranger picks); got {}",
            stats.hub_rejected_edges
        );
        // Every rejection got a replacement.
        assert_eq!(
            stats.replacements_added, stats.hub_rejected_edges,
            "expected one replacement per rejection; got {} replacements for {} rejections",
            stats.replacements_added, stats.hub_rejected_edges
        );
        // Spot-check: a rejected leaf is NOT connected to the hub.
        let hub = t.nidx(0);
        let kept_set: HashSet<NodeIndex> = t.peers.neighbors(hub).collect();
        let rejected_leaf = (1..=200u64)
            .find(|&i| !kept_set.contains(&t.nidx(i)))
            .expect("at least one leaf must be rejected");
        assert!(!t.peers.contains_edge(t.nidx(rejected_leaf), hub));
    }

    /// With the cap off, no edges are rejected and no replacements
    /// happen — both stats are zero.
    #[test]
    fn no_rejections_or_replacements_when_cap_off() {
        let chans: Vec<(NodeId, NodeId)> = (1..=200u64).map(|i| (0u64, i)).collect();
        let mut t = topo_with_channels(2000, &chans);
        let stats = build_peer_graph(&mut t, const_k(4), const_max_peer(100), 7, false);
        assert_eq!(stats.hub_rejected_edges, 0);
        assert_eq!(stats.replacements_added, 0);
    }

    /// 100 of node 0's channel counterparties (the ones it didn't pick)
    /// also lose their peer-edge to it under the cap. They still
    /// retain the channel — just not a peer connection.
    #[test]
    fn hub_cap_drops_unselected_counterparties_peer_edges() {
        let chans: Vec<(NodeId, NodeId)> = (1..=200u64).map(|i| (0u64, i)).collect();
        let mut t = topo_with_channels(220, &chans);
        build_peer_graph(&mut t, const_k(5), const_max_peer(100), 42, true);
        let nx0 = t.nidx(0);
        let kept_set: HashSet<NodeIndex> = t.peers.neighbors(nx0).collect();
        let mut dropped = 0usize;
        for i in 1..=200u64 {
            let nxi = t.nidx(i);
            if !kept_set.contains(&nxi) {
                dropped += 1;
                // Channel still exists — just no peer edge.
                assert!(
                    t.channels.find_edge(nxi, nx0).is_some()
                        || t.channels.find_edge(nx0, nxi).is_some()
                );
                assert!(!t.peers.contains_edge(nxi, nx0));
            }
        }
        assert_eq!(dropped, 100, "expected 100 counterparties dropped from peers");
    }

    /// A sparse leaf's final peer count stays close to `k`. With the
    /// `(k - c).div_ceil(2)` stranger-count rule, each picker draws
    /// half as many strangers as a naive `k - c` would suggest, so
    /// OR-semantics edge union no longer doubles the per-node degree
    /// to ≈ 2k. div_ceil keeps min-degree sane at small `(k, c)`
    /// (e.g. `k = 2, c = 1` still picks one stranger).
    #[test]
    fn sparse_node_close_to_k_total() {
        let chans = vec![(0u64, 1u64)];
        let mut t = topo_with_channels(20, &chans);
        let k = 4;
        build_peer_graph(&mut t, const_k(k), const_max_peer(100), 1, false);
        let nx0 = t.nidx(0);
        let peers: HashSet<NodeIndex> = t.peers.neighbors(nx0).collect();
        assert!(peers.contains(&t.nidx(1)), "must keep its only counterparty");
        assert!(!peers.contains(&nx0));
        let upper = k + 2;
        assert!(
            peers.len() <= upper,
            "expected total peer count close to k={k} (≤ {upper}); got {}",
            peers.len()
        );
    }

    /// `(k - c).div_ceil(2)` rounds up so the small-(k, c) corner
    /// (`k = 2, c = 1`) still produces a stranger pick — a leaf with
    /// one channel partner doesn't get stranded at degree 1.
    #[test]
    fn sparse_min_degree_with_small_k() {
        let chans = vec![(0u64, 1u64)];
        let mut t = topo_with_channels(50, &chans);
        let k = 2;
        build_peer_graph(&mut t, const_k(k), const_max_peer(100), 7, false);
        let nx0 = t.nidx(0);
        let peers: HashSet<NodeIndex> = t.peers.neighbors(nx0).collect();
        assert!(peers.contains(&t.nidx(1)), "must keep its only counterparty");
        assert!(
            peers.len() >= 2,
            "k=2, c=1 leaf should pick at least one stranger (got {} peers total)",
            peers.len()
        );
    }

    #[test]
    fn deterministic_for_same_seed() {
        let chans: Vec<(NodeId, NodeId)> = (1..=10u64).map(|i| (0u64, i)).collect();
        let mut t1 = topo_with_channels(50, &chans);
        let mut t2 = topo_with_channels(50, &chans);
        build_peer_graph(&mut t1, const_k(4), const_max_peer(100), 99, false);
        build_peer_graph(&mut t2, const_k(4), const_max_peer(100), 99, false);
        let p1: HashSet<(usize, usize)> = t1
            .peers
            .edge_indices()
            .map(|e| {
                let (a, b) = t1.peers.edge_endpoints(e).unwrap();
                let (lo, hi) = if a.index() < b.index() {
                    (a.index(), b.index())
                } else {
                    (b.index(), a.index())
                };
                (lo, hi)
            })
            .collect();
        let p2: HashSet<(usize, usize)> = t2
            .peers
            .edge_indices()
            .map(|e| {
                let (a, b) = t2.peers.edge_endpoints(e).unwrap();
                let (lo, hi) = if a.index() < b.index() {
                    (a.index(), b.index())
                } else {
                    (b.index(), a.index())
                };
                (lo, hi)
            })
            .collect();
        assert_eq!(p1, p2);
    }
}
