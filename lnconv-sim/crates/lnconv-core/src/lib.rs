//! Lightning Network gossip-propagation simulator core.
//!
//! See the workspace [`README`](../../../README.md) for the full picture.
//! Quick orientation:
//!
//! * [`sim::run`] is the top-level entry point — it builds the topology,
//!   spawns models for the configured algorithm, and drives the
//!   simulation while injecting scheduled events and emitting progress.
//! * [`node`] holds one `#[Model]` impl per gossip strategy:
//!   [`node::flooding`], [`node::cln`], [`node::lnd`].
//! * [`message::WireMessage`] is the shared inter-node wire type that
//!   makes mixed populations possible.
//! * [`topology`], [`events`], [`metrics`], [`config`] are supporting
//!   modules — see each one's docstring.

pub mod channels;
pub mod config;
pub mod events;
pub mod latency;
pub mod message;
pub mod metrics;
pub mod metrics_aggregator;
pub mod node;
pub mod sim;
pub mod state;
pub mod stats_writer;
pub mod topology;

pub use message::{
    Direction, Gossip, GossipKind, MsgId, NodeId, NodeIdx, Scid, Sketch, SketchKind, WireMessage,
};
pub use state::{NodeState, SharedNodeState};
