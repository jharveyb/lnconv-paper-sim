//! Topology generators.
//!
//! Currently only one — random k-regular. We delegate to rustworkx-core
//! (which itself wraps petgraph) and convert the resulting graph to our
//! flat `Vec<Vec<u32>>` adjacency list. petgraph types stay an
//! implementation detail; the rest of the crate sees only `Topology`.

use rustworkx_core::generators::random_regular_graph;
use rustworkx_core::petgraph::graph::UnGraph;
use rustworkx_core::petgraph::visit::EdgeRef;

use super::Topology;

/// True k-regular random graph: every node has exactly `k` neighbours,
/// chosen uniformly at random subject to no self-loops or parallel edges.
/// Built via rustworkx-core's `random_regular_graph` (configuration-model
/// based with rejection).
///
/// For LN-shaped experiments this is the realistic baseline: random
/// regular graphs are small-world (diameter ~`log_k(n)`, ~6-8 hops on
/// 20k nodes with k=6) and so propagation timings come out in the same
/// ballpark as the real network.
pub fn random_regular(n: usize, k: usize, seed: u64) -> Topology {
    let g: UnGraph<(), ()> =
        random_regular_graph(n, k, Some(seed), || (), || ()).expect("random_regular_graph");
    petgraph_to_adj(&g)
}

/// Flatten a petgraph undirected graph into our adjacency-list shape.
/// Each undirected edge appears twice — once in each endpoint's list.
fn petgraph_to_adj(g: &UnGraph<(), ()>) -> Topology {
    let n = g.node_count();
    let mut adj: Vec<Vec<u32>> = vec![Vec::new(); n];
    for edge in g.edge_references() {
        let u = edge.source().index() as u32;
        let v = edge.target().index() as u32;
        adj[u as usize].push(v);
        adj[v as usize].push(u);
    }
    adj
}
