//! Topology stats — degree, diameter, mean path length, connectedness,
//! reported separately for the **peer** graph (used by gossip
//! propagation) and the **channels** graph (the underlying LN payment
//! topology).
//!
//! Computed via [`petgraph::algo::dijkstra`] with unit edge weights.
//! Useful as a pre-flight check: propagation time for any algorithm
//! scales with the peer-graph diameter, so if the printed diameter
//! looks crazy (e.g. thousands) you're about to run a sim that won't
//! converge.
//!
//! The channels graph is internally a `DiGraph` (one directed edge per
//! channel; source = dir-0 owner) but propagation reality is
//! bidirectional, so we project to an undirected graph for stats.

use petgraph::algo::dijkstra;
use petgraph::graph::UnGraph;

use rand::SeedableRng;
use rand::seq::IndexedRandom;
use rand_chacha::ChaCha8Rng;

use super::Topology;
use crate::message::NodeId;

#[derive(Debug)]
pub struct TopologyMetrics {
    pub peers: GraphMetrics,
    pub channels: GraphMetrics,
}

#[derive(Debug)]
pub struct GraphMetrics {
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
    /// True iff the graph is a single connected component (within the
    /// sample).
    pub connected: bool,
    /// Largest reached component size (across the BFS sources).
    pub largest_component_seen: usize,
}

/// Compute peer-graph and channel-graph stats. The channel graph is
/// projected to an undirected view first (real LN channels are
/// bidirectional for propagation purposes).
pub fn compute(
    topo: &Topology,
    seed: u64,
    max_exact_n: usize,
    sample_sources: usize,
) -> TopologyMetrics {
    let peers = stats_undirected(&topo.peers, seed, max_exact_n, sample_sources);
    let channels_un = project_channels_undirected(topo);
    let channels = stats_undirected(&channels_un, seed ^ 0xC0FFEE, max_exact_n, sample_sources);
    TopologyMetrics { peers, channels }
}

/// Generic stats for any `UnGraph<N, E>` — works for the peer graph
/// directly, and for the projected channel graph after we drop edge
/// metadata.
fn stats_undirected<N, E>(
    g: &UnGraph<N, E>,
    seed: u64,
    max_exact_n: usize,
    sample_sources: usize,
) -> GraphMetrics {
    let n = g.node_count();
    let edges = g.edge_count();
    let degs: Vec<usize> = g.node_indices().map(|nx| g.neighbors(nx).count()).collect();
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
        let src_idx = petgraph::graph::NodeIndex::new(src as usize);
        let dists = dijkstra(g, src_idx, None, |_| 1u32);
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

    GraphMetrics {
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

/// Build a temporary undirected projection of `topo.channels`: same
/// vertices, one undirected edge per directed edge (parallel
/// edges in either direction collapse). NodeIndex is preserved.
fn project_channels_undirected(topo: &Topology) -> UnGraph<(), ()> {
    let mut un: UnGraph<(), ()> = UnGraph::with_capacity(topo.channels.node_count(), 0);
    for _ in 0..topo.channels.node_count() {
        un.add_node(());
    }
    use std::collections::HashSet;
    let mut seen: HashSet<(usize, usize)> = HashSet::new();
    for e in topo.channels.edge_indices() {
        let (a, b) = topo.channels.edge_endpoints(e).unwrap();
        let (lo, hi) = if a.index() < b.index() {
            (a.index(), b.index())
        } else {
            (b.index(), a.index())
        };
        if lo == hi {
            continue;
        }
        if seen.insert((lo, hi)) {
            un.add_edge(
                petgraph::graph::NodeIndex::new(lo),
                petgraph::graph::NodeIndex::new(hi),
                (),
            );
        }
    }
    un
}
