//! Event-stream generators.
//!
//! An event is a (delay, originating node, gossip message) tuple. The
//! simulator builds the full list at startup, sorts by delay, and
//! injects each one at its scheduled simulated time via the chunked
//! driver in [`crate::sim`].
//!
//! Originators are no longer chosen randomly per event; each event is
//! associated with a `(scid, direction)` from the channel registry, and
//! the registry knows which node owns that channel side. Every stream
//! takes a `&ChannelRegistry` so it can perform that lookup.
//!
//! Adding a new stream type means:
//! 1. Implement `EventSchedule::build` to return your tuples.
//! 2. Add a serde-tagged variant to `EventCfg` in `config.rs`.
//! 3. Add a match arm in `sim::build_events`.

pub mod oneshot;
pub mod poisson;

use std::time::Duration;

use crate::channels::ChannelRegistry;
use crate::message::{Gossip, NodeId};

/// An event schedule produces a list of (delay-from-t0, source-node, message)
/// tuples to inject into the simulation. `max_duration` lets unbounded
/// streams (e.g. rate-based) cap themselves to the run window. The
/// `registry` is consulted for `(scid, direction) -> originator` lookups.
pub trait EventSchedule {
    fn build(
        &self,
        num_nodes: usize,
        max_duration: Duration,
        registry: &ChannelRegistry,
    ) -> Vec<(Duration, NodeId, Gossip)>;
}
