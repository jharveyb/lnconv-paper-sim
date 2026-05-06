//! Topology generation and analysis.
//!
//! [`synthetic`] builds the peer-to-peer graph that nodes are wired
//! across. [`metrics`] then BFSes it to report degree stats, diameter,
//! mean path length, and connectedness — useful sanity checks before a
//! long run, since propagation timings scale roughly as
//! `diameter × per-hop-time`.

pub mod metrics;
pub mod synthetic;

/// Adjacency list: `topology[i]` is the list of node-IDs that node `i` is
/// connected to. The graph is undirected, so each edge appears in both
/// endpoints' lists. This is consumed once at sim init by `sim::run_*`
/// to call `Output::connect` for every (src, dst) edge; after that the
/// runtime never touches it.
pub type Topology = Vec<Vec<u32>>;
