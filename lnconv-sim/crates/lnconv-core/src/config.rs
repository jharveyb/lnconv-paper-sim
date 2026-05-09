//! TOML configuration schema.
//!
//! A run is fully determined by a `SimConfig` plus a global `seed`. See
//! the README for an annotated example. All randomness in the simulator
//! (topology, per-node tick phases, Poisson stream) is derived from
//! `seed` via xor'd subseeds, so two runs with the same TOML produce
//! bit-identical output.

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub topology: TopologyCfg,
    /// Optional when `topology.kind = "from_csv"` — the channel set is
    /// then determined by the CSV instead. Required for synthetic
    /// `topology.kind = "k_regular"`.
    #[serde(default)]
    pub channels: Option<ChannelsCfg>,
    pub latency: LatencyCfg,
    pub algo: AlgoCfg,
    pub event: EventCfg,
    pub run: RunCfg,
}

#[derive(Deserialize, Debug)]
pub struct ChannelsCfg {
    /// Total number of `(scid)` channels in the registry. Each channel
    /// has two directions, both owned by distinct random nodes assigned
    /// at sim init. Should typically be `>= num_nodes` so most nodes own
    /// at least one channel side.
    pub count: u32,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TopologyCfg {
    /// True k-regular random graph (no parallel edges, no self-loops).
    KRegular { n: usize, k: usize },
    /// Load nodes + channels from CSV files (real LN snapshot). The
    /// channel graph is taken verbatim; the peer graph is built from
    /// it via the `k`-driven rule (see `topology::synthetic::build_peer_graph`):
    ///
    /// - nodes with > 100 channel counterparties: keep 100 random ones as peers.
    /// - nodes with `k <= c <= 100` counterparties: keep all (no extra strangers).
    /// - nodes with `c < k` counterparties: keep all + `k - c - 1` strangers.
    ///
    /// `k` is per-impl-type — different gossip algorithms target
    /// different peer-degrees in real LN (LND nodes are typically
    /// thinner than CLN). For `algo.kind = "mix"`, each vertex's k is
    /// resolved from its assigned algorithm.
    ///
    /// `enforce_hub_cap` (default false): when true, hubs (`c > 100`)
    /// pre-commit to their 100 picks and any peer-edge into a hub from
    /// a node *not* in that pick set is silently dropped. Each dropped
    /// edge is replaced with a stranger in a phase-3 top-up so total
    /// edge count is preserved.
    FromCsv {
        nodes_csv: PathBuf,
        channels_csv: PathBuf,
        k: KByAlgo,
        #[serde(default)]
        enforce_hub_cap: bool,
    },
}

/// Per-impl-type peer-build threshold `k`. Used by `FromCsv` topology
/// to give different gossip algorithms different target peer-degrees.
#[derive(Deserialize, Debug, Clone)]
pub struct KByAlgo {
    pub flooding: usize,
    pub cln: usize,
    pub lnd: usize,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "dist", rename_all = "snake_case")]
pub enum LatencyCfg {
    Constant { ms: u64 },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AlgoCfg {
    Flooding {},
    Cln {
        stagger_ms: u64,
    },
    Lnd {
        stagger_ms: u64,
        trickle_ms: u64,
        min_batch_size: usize,
    },
    /// Heterogeneous mix of stagger algorithms. `population` lists fractions
    /// (must sum to 1.0); each node is deterministically assigned a kind
    /// based on a shuffle seeded by `cfg.seed`.
    Mix {
        population: Vec<MixEntry>,
    },
}

#[derive(Deserialize, Debug, Clone)]
pub struct MixEntry {
    pub fraction: f64,
    pub algo: NodeAlgoKind,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeAlgoKind {
    Cln {
        stagger_ms: u64,
    },
    Lnd {
        stagger_ms: u64,
        trickle_ms: u64,
        min_batch_size: usize,
    },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventCfg {
    OneShotSingle {
        /// Vertex *index* into the topology's NodeId order (0..n). For
        /// synthetic configs this matches the NodeId numerically; for
        /// CSV-loaded configs it picks the n-th row of `node_list.csv`.
        node: usize,
    },
    OneShotAll {},
    /// Poisson-process stream of `rate_per_sec` messages from random nodes,
    /// running for the full simulation duration.
    PoissonRandom {
        rate_per_sec: f64,
        #[serde(default = "default_msg_size")]
        size_bytes: u32,
    },
}

fn default_msg_size() -> u32 {
    1024
}

#[derive(Deserialize, Debug)]
pub struct RunCfg {
    pub duration_seconds: u64,
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,
    /// How often to print a sim-time/wall-time progress line, in simulated
    /// seconds. 0 disables progress output.
    #[serde(default = "default_progress_interval")]
    pub progress_interval_seconds: u64,
    /// NeXosim executor worker-thread count. `None` (the default) lets
    /// NeXosim use all logical cores. The CLI's `--threads` flag
    /// overrides this when set.
    #[serde(default)]
    pub threads: Option<usize>,
}

fn default_mailbox_capacity() -> usize {
    1024
}

fn default_progress_interval() -> u64 {
    10
}

impl SimConfig {
    pub fn from_path(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}
