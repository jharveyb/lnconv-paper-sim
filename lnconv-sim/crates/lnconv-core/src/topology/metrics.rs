//! Topology stats — degree, diameter, mean path length, connectedness.
//!
//! Computed via [`petgraph::algo::dijkstra`] on the peer graph (unit
//! edge weights). Useful as a pre-flight check: propagation time for any
//! algorithm scales with diameter, so if the printed diameter looks
//! crazy (e.g. thousands) you're about to run a sim that won't converge.

use petgraph::algo::dijkstra;

use rand::SeedableRng;
use rand::seq::IndexedRandom;
use rand_chacha::ChaCha8Rng;

use super::Topology;
use crate::message::NodeId;

#[derive(Debug)]
pub struct TopologyMetrics {
    pub n: usize,
    pub edges: usize,
    pub min_degree: usize,
    pub max_degree: usize,
    pub mean_degree: f64,
    pub diameter: u32,
    pub mean_path_length: f64,
    /// Number of source nodes BFS'd from; equal to `n` when `exact` is true.
    pub bfs_sources: usize,
    pub exact: bool,
    /// True iff the graph is a single connected component.
    pub connected: bool,
    /// Largest reached component size (across the BFS sources).
    pub largest_component_seen: usize,
}

/// Compute topology stats. BFS exhaustively when `n <= max_exact_n` —
/// O(N · (N + E)) and tractable to a few thousand nodes. For larger
/// graphs, sample `sample_sources` random source nodes; the resulting
/// diameter is a *lower bound* (true diameter could be larger if both
/// endpoints of the longest shortest path were missed) but tight enough
/// for a sanity check at our typical sample size.
pub fn compute(
    topo: &Topology,
    seed: u64,
    max_exact_n: usize,
    sample_sources: usize,
) -> TopologyMetrics {
    let n = topo.len();
    let edges = topo.peers.edge_count();
    let degs: Vec<usize> = topo
        .peers
        .node_indices()
        .map(|nx| topo.peers.neighbors(nx).count())
        .collect();
    let min_degree = *degs.iter().min().unwrap_or(&0);
    let max_degree = *degs.iter().max().unwrap_or(&0);
    let mean_degree = degs.iter().sum::<usize>() as f64 / n.max(1) as f64;

    let exact = n <= max_exact_n;
    let sources: Vec<NodeId> = if exact {
        (0..n as NodeId).collect()
    } else {
        let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xBF5);
        let pool: Vec<NodeId> = (0..n as NodeId).collect();
        pool.choose_multiple(&mut rng, sample_sources.min(n))
            .copied()
            .collect()
    };

    let mut max_dist = 0u32;
    let mut total_dist: u64 = 0;
    let mut total_pairs: u64 = 0;
    let mut largest_component = 0usize;
    let mut connected = true;
    for &src in &sources {
        // dijkstra with unit edge weights == BFS distance, with the
        // benefit of a tested implementation. Returns a HashMap of
        // NodeIndex → distance for every reachable node.
        let dists = dijkstra(&topo.peers, Topology::nidx(src), None, |_| 1u32);
        let reachable = dists.len();
        for &d in dists.values() {
            max_dist = max_dist.max(d);
            total_dist += d as u64;
            total_pairs += 1;
        }
        largest_component = largest_component.max(reachable);
        if reachable < n {
            connected = false;
        }
    }

    let mean_path_length = if total_pairs > 0 {
        total_dist as f64 / total_pairs as f64
    } else {
        0.0
    };

    TopologyMetrics {
        n,
        edges,
        min_degree,
        max_degree,
        mean_degree,
        diameter: max_dist,
        mean_path_length,
        bfs_sources: sources.len(),
        exact,
        connected,
        largest_component_seen: largest_component,
    }
}
