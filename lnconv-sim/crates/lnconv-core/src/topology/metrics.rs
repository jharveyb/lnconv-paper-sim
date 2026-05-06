//! Topology stats — degree, diameter, mean path length, connectedness.
//!
//! Computed via BFS from each (or a sample of) source node. Useful as a
//! pre-flight check: propagation time for any algorithm scales with
//! diameter, so if the printed diameter looks crazy (e.g. thousands)
//! you're about to run a sim that won't converge.

use std::collections::VecDeque;

use rand::SeedableRng;
use rand::seq::IndexedRandom;
use rand_chacha::ChaCha8Rng;

use super::Topology;

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
    let edges = topo.iter().map(|p| p.len()).sum::<usize>() / 2;
    let degs: Vec<usize> = topo.iter().map(|p| p.len()).collect();
    let min_degree = *degs.iter().min().unwrap_or(&0);
    let max_degree = *degs.iter().max().unwrap_or(&0);
    let mean_degree = degs.iter().sum::<usize>() as f64 / n.max(1) as f64;

    let exact = n <= max_exact_n;
    let sources: Vec<u32> = if exact {
        (0..n as u32).collect()
    } else {
        let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xBF5);
        let pool: Vec<u32> = (0..n as u32).collect();
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
        let dist = bfs_distances(topo, src);
        let mut reachable = 0usize;
        for &d in &dist {
            if d != u32::MAX {
                max_dist = max_dist.max(d);
                total_dist += d as u64;
                total_pairs += 1;
                reachable += 1;
            }
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

fn bfs_distances(topo: &Topology, src: u32) -> Vec<u32> {
    let n = topo.len();
    let mut dist = vec![u32::MAX; n];
    dist[src as usize] = 0;
    let mut q = VecDeque::new();
    q.push_back(src);
    while let Some(u) = q.pop_front() {
        let d = dist[u as usize];
        for &v in &topo[u as usize] {
            if dist[v as usize] == u32::MAX {
                dist[v as usize] = d + 1;
                q.push_back(v);
            }
        }
    }
    dist
}
