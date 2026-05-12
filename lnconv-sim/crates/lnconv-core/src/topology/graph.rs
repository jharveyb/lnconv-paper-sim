//! `Topology` — peer-to-peer graph + channel graph, both backed by petgraph.
//!
//! Two graphs share the same NodeIndex space (insertion order, dense
//! `0..n`):
//!
//! * **`peers`** — undirected `UnGraph<NodeMeta, ()>`. An edge `(u, v)`
//!   means u and v exchange gossip directly (a "peer connection").
//!   Vertex weight carries the node's `id` (u64; pubkey-hash for CSV
//!   loads or dense 0..n for synthetic), `idx` (dense `NodeIdx` 0..n
//!   used for `Vec`-indexed metric storage), and `algo` (per-vertex
//!   algorithm assignment).
//!
//! * **`channels`** — directed `DiGraph<(), Scid>`. Each Lightning
//!   channel is exactly **one** directed edge: `dir0_owner → dir1_owner`
//!   carrying `Scid`. From a node's perspective, outgoing edges == its
//!   direction-0 ownerships, incoming edges == direction-1.
//!
//! Channels and the peer graph stay independent for now (a channel
//! between u and v doesn't imply a peer edge between u and v).
//!
//! ## Sparse NodeId
//!
//! For CSV-loaded snapshots, NodeId is `xxhash64(pubkey)` — an arbitrary
//! u64. The `id_to_nidx` HashMap maps any NodeId back to its
//! `NodeIndex` in the peer graph in O(1). Synthetic configs hit it just
//! the same; the lookup is cheap and keeps a single code path.

use std::collections::HashMap;

use petgraph::Direction as PgDirection;
use petgraph::graph::{DiGraph, NodeIndex, UnGraph};

use crate::config::NodeAlgoKind;
use crate::message::{Direction, NodeId, NodeIdx, Scid};

/// Per-vertex metadata. Stored on every `peers` node so iteration
/// returns id + idx + algo in one place.
#[derive(Clone, Debug)]
pub struct NodeMeta {
    /// Stable identifier. Hash of pubkey for CSV loads (sparse u64);
    /// dense 0..n cast to u64 for synthetic.
    pub id: NodeId,
    /// Dense per-node index `0..n`, equal to `NodeIndex::index() as u32`.
    /// Use this for `Vec`-indexed storage (e.g. metrics' coverage
    /// vector). Distinct from `id` because `id` may be sparse.
    pub idx: NodeIdx,
    pub algo: NodeAlgo,
}

/// All possible per-node algorithms. Superset of the config-side
/// `NodeAlgoKind`, which only covers stagger algos (Cln/Lnd/Sketch) —
/// this enum also includes Flooding so a homogeneous flooding
/// population can use the same `Topology` shape as a Cln/Lnd/Mix run.
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
    /// Set-reconciliation node. Doesn't fan-out gossip on recv —
    /// state propagates only via per-peer sketch exchanges over
    /// **all three kinds**, each with its own capacity.
    Sketch {
        stagger_ms: u64,
        capacity_chan_updates: u32,
        capacity_node_anns: u32,
        capacity_chan_anns: u32,
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
            NodeAlgoKind::Sketch {
                stagger_ms,
                capacity_chan_updates,
                capacity_node_anns,
                capacity_chan_anns,
            } => NodeAlgo::Sketch {
                stagger_ms: *stagger_ms,
                capacity_chan_updates: *capacity_chan_updates,
                capacity_node_anns: *capacity_node_anns,
                capacity_chan_anns: *capacity_chan_anns,
            },
        }
    }
}

pub struct Topology {
    pub peers: UnGraph<NodeMeta, ()>,
    pub channels: DiGraph<(), Scid>,
    /// O(1) lookup from `NodeId` to `NodeIndex`. Populated alongside
    /// each `add_node` call.
    id_to_nidx: HashMap<NodeId, NodeIndex>,
}

impl Topology {
    /// Build an empty topology with `n` vertices using sequential
    /// `NodeId = 0..n` (dense, cast to u64). All vertices are tagged
    /// with `default_algo`; the runner mutates per-vertex algo afterwards
    /// for `Mix` populations.
    pub fn empty(n: usize, default_algo: NodeAlgo) -> Self {
        let mut topo = Self {
            peers: UnGraph::with_capacity(n, 0),
            channels: DiGraph::with_capacity(n, 0),
            id_to_nidx: HashMap::with_capacity(n),
        };
        for i in 0..n {
            topo.add_node(i as NodeId, default_algo.clone());
        }
        topo
    }

    /// Build an empty topology with no vertices yet — caller is
    /// responsible for calling `add_node` for each vertex it wants.
    /// Used by the CSV loader when NodeIds aren't known until each
    /// pubkey is hashed.
    pub fn with_capacity(n_hint: usize) -> Self {
        Self {
            peers: UnGraph::with_capacity(n_hint, 0),
            channels: DiGraph::with_capacity(n_hint, 0),
            id_to_nidx: HashMap::with_capacity(n_hint),
        }
    }

    /// Insert a vertex with the given `id` and `algo`. The `idx` field
    /// on `NodeMeta` is the resulting `NodeIndex.index() as u32`.
    /// Asserts that `id` is unique (collisions on the hash space would
    /// otherwise silently overwrite a vertex's identity).
    pub fn add_node(&mut self, id: NodeId, algo: NodeAlgo) -> NodeIndex {
        assert!(
            !self.id_to_nidx.contains_key(&id),
            "duplicate NodeId {id} (hash collision? try changing the seed)"
        );
        let nx = self.peers.add_node(NodeMeta {
            id,
            idx: 0, // patched immediately after to match NodeIndex
            algo,
        });
        // Channels graph gets a parallel vertex so NodeIndex matches.
        let nx_ch = self.channels.add_node(());
        debug_assert_eq!(
            nx, nx_ch,
            "peers and channels graphs out of sync — both must add nodes in lockstep"
        );
        self.peers[nx].idx = nx.index() as NodeIdx;
        self.id_to_nidx.insert(id, nx);
        nx
    }

    pub fn len(&self) -> usize {
        self.peers.node_count()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.node_count() == 0
    }

    /// Convert a `NodeId` to a `NodeIndex`. Looks up via the internal
    /// HashMap so sparse u64 ids work the same as dense ones.
    #[inline]
    pub fn nidx(&self, id: NodeId) -> NodeIndex {
        self.id_to_nidx[&id]
    }

    /// Same as `nidx` but returns `None` if the id is unknown.
    #[inline]
    pub fn nidx_opt(&self, id: NodeId) -> Option<NodeIndex> {
        self.id_to_nidx.get(&id).copied()
    }

    pub fn node_meta(&self, id: NodeId) -> &NodeMeta {
        &self.peers[self.nidx(id)]
    }

    pub fn node_meta_mut(&mut self, id: NodeId) -> &mut NodeMeta {
        let nx = self.nidx(id);
        &mut self.peers[nx]
    }

    /// Iterate all `NodeId`s in insertion order.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.peers.node_indices().map(|nx| self.peers[nx].id)
    }

    /// Iterate the `NodeId`s connected to `id` over the peer graph.
    pub fn peer_ids(&self, id: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        let nx = self.nidx(id);
        self.peers.neighbors(nx).map(move |ny| self.peers[ny].id)
    }

    /// Add a peer-graph edge between `a` and `b`. Caller is responsible
    /// for deduping if avoiding parallel edges matters (use
    /// `peers.contains_edge(...)`).
    pub fn add_peer_edge(&mut self, a: NodeId, b: NodeId) {
        let na = self.nidx(a);
        let nb = self.nidx(b);
        self.peers.add_edge(na, nb, ());
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
        let n0 = self.nidx(dir0_owner);
        let n1 = self.nidx(dir1_owner);
        self.channels.add_edge(n0, n1, scid);
    }

    /// Iterate `(scid, direction)` channel-sides owned by `node`.
    /// Outgoing edges == direction 0; incoming edges == direction 1.
    pub fn channels_for(&self, node: NodeId) -> impl Iterator<Item = (Scid, Direction)> + '_ {
        let nx = self.nidx(node);
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
