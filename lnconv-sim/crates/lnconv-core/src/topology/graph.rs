//! `Topology` — peer-to-peer graph + channel graph, both backed by petgraph.
//!
//! Two graphs share the same NodeIndex space (insertion order matches
//! `NodeId 0..n`):
//!
//! * **`peers`** — undirected `UnGraph<NodeMeta, ()>`. An edge `(u, v)`
//!   means u and v exchange gossip directly (a "peer connection"). Vertex
//!   weight carries the node's id and algorithm assignment, so callers
//!   can iterate vertices and dispatch on algo without a parallel
//!   `Vec<NodeAlgoKind>`.
//!
//! * **`channels`** — directed `DiGraph<(), Scid>`. Each Lightning
//!   channel becomes *two* directed edges, one per direction. Edge
//!   `u → v` carrying `Scid s` means node `u` is the owner of
//!   `(scid=s, direction=0)` and node `v` is the owner of
//!   `(scid=s, direction=1)`. The "directions are opposite-endpoint"
//!   invariant is structural — you can't accidentally give one node
//!   both directions.
//!
//! Channels and the peer graph stay independent for now (a channel
//! between u and v doesn't imply a peer edge between u and v). Future
//! work may couple them.

use petgraph::Direction as PgDirection;
use petgraph::graph::{DiGraph, NodeIndex, UnGraph};

use crate::config::NodeAlgoKind;
use crate::message::{Direction, NodeId, Scid};

/// Per-vertex metadata. Stored on every `peers` node so iteration
/// returns id + algo without needing a separate vector.
#[derive(Clone, Debug)]
pub struct NodeMeta {
    pub id: NodeId,
    pub algo: NodeAlgo,
}

/// All possible per-node algorithms. Superset of the config-side
/// `NodeAlgoKind`, which only covers stagger algos (Cln/Lnd) — this enum
/// also includes Flooding so a homogeneous flooding population can use
/// the same `Topology` shape as a Cln/Lnd/Mix run.
#[derive(Clone, Debug)]
pub enum NodeAlgo {
    Flooding,
    Cln {
        stagger_ms: u64,
    },
    Lnd {
        stagger_ms: u64,
        trickle_ms: u64,
        min_batch_size: usize,
    },
}

impl From<&NodeAlgoKind> for NodeAlgo {
    fn from(k: &NodeAlgoKind) -> Self {
        match k {
            NodeAlgoKind::Cln { stagger_ms } => NodeAlgo::Cln {
                stagger_ms: *stagger_ms,
            },
            NodeAlgoKind::Lnd {
                stagger_ms,
                trickle_ms,
                min_batch_size,
            } => NodeAlgo::Lnd {
                stagger_ms: *stagger_ms,
                trickle_ms: *trickle_ms,
                min_batch_size: *min_batch_size,
            },
        }
    }
}

pub struct Topology {
    pub peers: UnGraph<NodeMeta, ()>,
    pub channels: DiGraph<(), Scid>,
}

impl Topology {
    /// Build an empty topology with `n` vertices, all assigned
    /// `default_algo`. The runner mutates per-vertex algo afterwards
    /// when the configured algo is `Mix`.
    pub fn empty(n: usize, default_algo: NodeAlgo) -> Self {
        let mut peers: UnGraph<NodeMeta, ()> = UnGraph::with_capacity(n, 0);
        let mut channels: DiGraph<(), Scid> = DiGraph::with_capacity(n, 0);
        for i in 0..n {
            peers.add_node(NodeMeta {
                id: i as NodeId,
                algo: default_algo.clone(),
            });
            channels.add_node(());
        }
        Self { peers, channels }
    }

    pub fn len(&self) -> usize {
        self.peers.node_count()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.node_count() == 0
    }

    /// Convert a `NodeId` to a `NodeIndex`. Both graphs use the same
    /// index space because vertices are inserted in order.
    #[inline]
    pub fn nidx(id: NodeId) -> NodeIndex {
        NodeIndex::new(id as usize)
    }

    pub fn node_meta(&self, id: NodeId) -> &NodeMeta {
        &self.peers[Self::nidx(id)]
    }

    pub fn node_meta_mut(&mut self, id: NodeId) -> &mut NodeMeta {
        &mut self.peers[Self::nidx(id)]
    }

    /// Iterate all `NodeId` in insertion order (0..n).
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.peers.node_indices().map(|nx| nx.index() as NodeId)
    }

    /// Iterate the `NodeId`s connected to `id` over the peer graph.
    pub fn peer_ids(&self, id: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.peers
            .neighbors(Self::nidx(id))
            .map(|nx| nx.index() as NodeId)
    }

    /// Add a peer-graph edge between `a` and `b`. Idempotent in the
    /// sense that `petgraph` will allow parallel edges; callers should
    /// dedup themselves if needed.
    pub fn add_peer_edge(&mut self, a: NodeId, b: NodeId) {
        self.peers.add_edge(Self::nidx(a), Self::nidx(b), ());
    }

    /// Add a channel as a single directed edge `dir0_owner → dir1_owner`
    /// carrying `scid`. The directionality of the edge encodes the
    /// "which endpoint owns which direction" invariant structurally: an
    /// outgoing edge from N means N is the dir-0 owner; an incoming
    /// edge into N means N is the dir-1 owner.
    pub fn add_channel(&mut self, scid: Scid, dir0_owner: NodeId, dir1_owner: NodeId) {
        debug_assert_ne!(
            dir0_owner, dir1_owner,
            "a channel's two endpoints must be distinct nodes"
        );
        self.channels
            .add_edge(Self::nidx(dir0_owner), Self::nidx(dir1_owner), scid);
    }

    /// Iterate `(scid, direction)` channel-sides owned by `node`.
    /// Outgoing edges == direction 0; incoming edges == direction 1.
    pub fn channels_for(&self, node: NodeId) -> impl Iterator<Item = (Scid, Direction)> + '_ {
        let nx = Self::nidx(node);
        let outgoing = self
            .channels
            .edges_directed(nx, PgDirection::Outgoing)
            .map(|e| (*e.weight(), 0u8));
        let incoming = self
            .channels
            .edges_directed(nx, PgDirection::Incoming)
            .map(|e| (*e.weight(), 1u8));
        outgoing.chain(incoming)
    }
}
