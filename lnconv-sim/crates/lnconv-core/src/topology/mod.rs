//! Topology generation and analysis.
//!
//! [`synthetic`] builds the peer-to-peer graph that nodes are wired
//! across. [`metrics`] then computes degree stats, diameter, mean path
//! length, and connectedness via petgraph — useful sanity checks before
//! a long run, since propagation timings scale roughly as
//! `diameter × per-hop-time`.
//!
//! The [`Topology`] type is two petgraph graphs sharing one NodeIndex
//! space: a `peers` graph (who exchanges gossip with whom) and a
//! `channels` graph (which channel-sides each node owns). See
//! [`graph`] for details.

pub mod graph;
pub mod metrics;
pub mod synthetic;

pub use graph::{NodeAlgo, NodeMeta, Topology};
