//! TOML configuration schema.
//!
//! A run is fully determined by a `SimConfig` plus a global `seed`. See
//! the README for an annotated example. All randomness in the simulator
//! (topology, per-node tick phases, Poisson stream) is derived from
//! `seed` via xor'd subseeds, so two runs with the same TOML produce
//! bit-identical output.

use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub topology: TopologyCfg,
    pub latency: LatencyCfg,
    pub algo: AlgoCfg,
    pub event: EventCfg,
    pub run: RunCfg,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TopologyCfg {
    /// True k-regular random graph (no parallel edges, no self-loops).
    KRegular { n: usize, k: usize },
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
        node: u32,
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
