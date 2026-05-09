//! CSV loader for real Lightning Network snapshots.
//!
//! Two CSVs are expected:
//!
//! * **`node_list.csv`** — single column `pubkey` (66-hex compressed
//!   pubkey).
//! * **`channel_list.csv`** — `scid,node_1,node_2`. `scid` is decimal
//!   u64-encoded; `node_1` / `node_2` are pubkeys that must appear in
//!   `node_list.csv`.
//!
//! ## NodeId / Scid derivation
//!
//! Real LN pubkeys are 33-byte compressed-secp256k1 points; SCIDs are
//! 64-bit packed (block height / tx index / output index). We don't
//! need either form internally — the simulator only needs *stable
//! identifiers* that are u64. The loader derives them via xxhash3_64
//! over the literal CSV string fields with a per-run seed:
//!
//! ```text
//! NodeId = xxhash3_64(seed ^ NODE_SUBSEED, pubkey)
//! Scid   = xxhash3_64(seed ^ SCID_SUBSEED, scid_string)
//! ```
//!
//! With u64 hashes, birthday-collision probability is negligible
//! (~1.6e-11 for 12k nodes / ~5.0e-11 for 42k channels). On the
//! exceedingly unlikely event of a collision, the loader returns
//! `Err(LoadError::HashCollision { .. })` with both colliding inputs
//! and the seed; the user changes `cfg.seed` and retries.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::Path;

use serde::Deserialize;
use twox_hash::xxhash3_64::Hasher as XX3Hasher;

use crate::message::{NodeId, Scid};

const NODE_SUBSEED: u64 = 0x4E4F44455F535542; // "NODE_SUB"
const SCID_SUBSEED: u64 = 0x534349445F535542; // "SCID_SUB"

/// Hash a 66-hex pubkey string into a stable `NodeId`. Single source
/// of truth used by both the CSV loader and the parquet replay
/// loader; same `seed` ⇒ same `NodeId` for the same pubkey.
pub fn hash_pubkey(seed: u64, pubkey: &str) -> NodeId {
    XX3Hasher::oneshot_with_seed(seed ^ NODE_SUBSEED, pubkey.as_bytes())
}

/// Hash an SCID *string* (decimal form) into a stable `Scid`. The CSV
/// snapshot ships SCIDs as decimal strings (e.g. `"1012232394786537478"`)
/// and the parquet trace ships them as `u64` — the parquet loader must
/// `to_string()` its `u64` before calling this so the resulting
/// `Scid` matches the CSV-derived one. Hashing raw `u64::to_le_bytes`
/// produces a different value and silently breaks registry lookups.
pub fn hash_scid_string(seed: u64, scid_str: &str) -> Scid {
    XX3Hasher::oneshot_with_seed(seed ^ SCID_SUBSEED, scid_str.as_bytes())
}

#[derive(Debug)]
pub struct LnSnapshot {
    /// Hash-derived NodeId for each pubkey, in CSV row order. Index
    /// in this Vec is the dense `NodeIdx` once the topology is built.
    pub nodes: Vec<NodeId>,
    /// Channels in CSV row order. Both endpoints are guaranteed to
    /// exist in `nodes` (the loader errors on unknown pubkeys).
    pub channels: Vec<(Scid, NodeId, NodeId)>,
    /// Reverse-lookup: hash → original pubkey string. Kept for
    /// debugging and for potential CLI hash-pretty-printing.
    pub pubkey_of: HashMap<NodeId, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("io error reading {path}: {source}")]
    Io {
        path: String,
        source: io::Error,
    },
    #[error("csv parse error in {path}: {source}")]
    Csv {
        path: String,
        source: csv::Error,
    },
    #[error(
        "xxhash64 collision on pubkeys '{a}' and '{b}' with seed {seed} \
         (probability ~5e-12 — try changing cfg.seed)"
    )]
    PubkeyHashCollision { a: String, b: String, seed: u64 },
    #[error(
        "xxhash64 collision on SCIDs '{a}' and '{b}' with seed {seed} \
         (probability ~2e-11 — try changing cfg.seed)"
    )]
    ScidHashCollision { a: String, b: String, seed: u64 },
    #[error("unknown pubkey '{pubkey}' on row {row} of {path}")]
    UnknownPubkey {
        path: String,
        row: usize,
        pubkey: String,
    },
}

#[derive(Debug, Deserialize)]
struct NodeRow {
    pubkey: String,
}

#[derive(Debug, Deserialize)]
struct ChannelRow {
    scid: String,
    node_1: String,
    node_2: String,
}

/// Load both CSVs and return a `LnSnapshot`. `seed` is the global
/// `cfg.seed`; the loader xors it with internal sub-seeds before
/// hashing so changing `cfg.seed` reshuffles all derived ids.
pub fn load(
    nodes_csv: &Path,
    channels_csv: &Path,
    seed: u64,
) -> Result<LnSnapshot, LoadError> {
    // 1. Parse pubkeys, hash, detect collisions.
    let mut nodes: Vec<NodeId> = Vec::new();
    let mut pubkey_of: HashMap<NodeId, String> = HashMap::new();
    let mut pubkey_to_id: HashMap<String, NodeId> = HashMap::new();
    let mut node_reader = csv::Reader::from_reader(open(nodes_csv)?);
    for record in node_reader.deserialize::<NodeRow>() {
        let row = record.map_err(|e| LoadError::Csv {
            path: nodes_csv.display().to_string(),
            source: e,
        })?;
        let id = hash_pubkey(seed, &row.pubkey);
        if let Some(existing) = pubkey_of.get(&id)
            && *existing != row.pubkey
        {
            return Err(LoadError::PubkeyHashCollision {
                a: existing.clone(),
                b: row.pubkey,
                seed,
            });
        }
        if pubkey_to_id.insert(row.pubkey.clone(), id).is_none() {
            nodes.push(id);
            pubkey_of.insert(id, row.pubkey);
        }
        // If pubkey was already in pubkey_to_id, the CSV had a
        // duplicate row — silently dedupe.
    }

    // 2. Parse channels, hash SCIDs, detect collisions, resolve pubkeys.
    let mut channels: Vec<(Scid, NodeId, NodeId)> = Vec::new();
    let mut scid_seen: HashMap<Scid, String> = HashMap::new();
    let mut chan_reader = csv::Reader::from_reader(open(channels_csv)?);
    for (row_idx, record) in chan_reader.deserialize::<ChannelRow>().enumerate() {
        let row = record.map_err(|e| LoadError::Csv {
            path: channels_csv.display().to_string(),
            source: e,
        })?;
        let scid = hash_scid_string(seed, &row.scid);
        if let Some(existing) = scid_seen.get(&scid)
            && *existing != row.scid
        {
            return Err(LoadError::ScidHashCollision {
                a: existing.clone(),
                b: row.scid,
                seed,
            });
        }
        scid_seen.insert(scid, row.scid.clone());
        let n1 = pubkey_to_id
            .get(&row.node_1)
            .copied()
            .ok_or_else(|| LoadError::UnknownPubkey {
                path: channels_csv.display().to_string(),
                row: row_idx + 2, // +1 for 0-based, +1 for header
                pubkey: row.node_1.clone(),
            })?;
        let n2 = pubkey_to_id
            .get(&row.node_2)
            .copied()
            .ok_or_else(|| LoadError::UnknownPubkey {
                path: channels_csv.display().to_string(),
                row: row_idx + 2,
                pubkey: row.node_2.clone(),
            })?;
        if n1 != n2 {
            channels.push((scid, n1, n2));
        }
        // n1 == n2 means a self-loop in the source data — skip.
    }

    Ok(LnSnapshot {
        nodes,
        channels,
        pubkey_of,
    })
}

fn open(path: &Path) -> Result<File, LoadError> {
    File::open(path).map_err(|e| LoadError::Io {
        path: path.display().to_string(),
        source: e,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_path(name: &str) -> std::path::PathBuf {
        // Tests run from the workspace root; init_data is at
        // `lnconv-sim/init_data/`.
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("init_data")
            .join(name)
    }

    #[test]
    fn loads_known_dataset() {
        let snap = load(
            &data_path("node_list.csv"),
            &data_path("channel_list.csv"),
            1,
        )
        .expect("load failed");
        assert_eq!(snap.nodes.len(), 11875, "expected 11875 nodes");
        assert_eq!(snap.channels.len(), 42123, "expected 42123 channels");
        // pubkey_of round-trips for every node.
        for &id in &snap.nodes {
            assert!(snap.pubkey_of.contains_key(&id));
        }
    }

    #[test]
    fn channels_reference_known_nodes() {
        let snap = load(
            &data_path("node_list.csv"),
            &data_path("channel_list.csv"),
            1,
        )
        .expect("load failed");
        let node_set: std::collections::HashSet<_> = snap.nodes.iter().copied().collect();
        for (_scid, n1, n2) in &snap.channels {
            assert!(node_set.contains(n1));
            assert!(node_set.contains(n2));
            assert_ne!(n1, n2);
        }
    }
}
