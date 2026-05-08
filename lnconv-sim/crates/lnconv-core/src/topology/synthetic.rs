//! Topology generators.
//!
//! Currently only one — random k-regular. We delegate to rustworkx-core
//! (which itself wraps petgraph) and copy its edges into our `Topology`'s
//! `peers` graph. The vertex metadata is filled with `default_algo` here;
//! the runner mutates per-vertex algo afterwards when the configured algo
//! is `Mix`.

use rustworkx_core::generators::random_regular_graph;
use rustworkx_core::petgraph::graph::UnGraph as RxUnGraph;
use rustworkx_core::petgraph::visit::EdgeRef;

use super::{NodeAlgo, Topology};

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
        let u = edge.source().index() as u32;
        let v = edge.target().index() as u32;
        topo.add_peer_edge(u, v);
    }
    topo
}
