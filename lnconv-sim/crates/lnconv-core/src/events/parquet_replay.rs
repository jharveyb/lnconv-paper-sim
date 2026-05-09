//! Replay events from a real-world ZSTD-compressed parquet capture.
//!
//! ## Schema
//!
//! ```text
//! first_seen_timestamp : timestamp(us, tz) — wall-clock first-seen
//! outer_hash, inner_hash : uint64 — IGNORED
//! type : int16 — 1=channel_announcement, 2=node_announcement,
//!                3=channel_update
//! size : int32 — message bytes (clamped into u16)
//! orig_node : varchar — 66-hex pubkey, populated for type=2 only
//! scid : uint64 — short channel id (decimal), populated for type=1/3 only
//! ```
//!
//! ## ID hashing
//!
//! Pubkeys and SCIDs are re-hashed via
//! [`crate::topology::ln_data::hash_pubkey`] /
//! [`hash_scid_string`](crate::topology::ln_data::hash_scid_string) so
//! the resulting `NodeId` / `Scid` values land in the same space the
//! CSV-loaded topology lives in. The parquet ships SCIDs as `u64`; we
//! `to_string()` first because the CSV loader hashes the literal
//! decimal string — hashing `u64::to_le_bytes` directly would silently
//! produce a different `Scid`.
//!
//! ## Time anchor
//!
//! Rows are guaranteed ascending by `first_seen_timestamp`. The first
//! row maps to sim t=0; every subsequent row's delay is
//! `(this_us - first_us)` micros. We BREAK out of the read loop the
//! moment the computed delay exceeds `max_duration` — no need to
//! materialise the rest of the trace.
//!
//! ## Per-row emission
//!
//! * `type=3` (channel_update) — pick direction via per-SCID
//!   round-robin (alternating 0,1,0,1 within a SCID). The originator
//!   is `registry.owner(scid, dir)`. Note: the parquet does not carry
//!   the real BOLT 7 channel_flags direction bit, so the rotor is a
//!   stand-in — per-direction asymmetry metrics on parquet replays are
//!   NOT meaningful.
//! * `type=2` (node_announcement) — origin = `hash_pubkey(seed,
//!   orig_node)`. Single tuple per row.
//! * `type=1` (channel_announcement) — emit TWO tuples at the same
//!   delay, one per `registry.owner(scid, 0)` and `(scid, 1)`. Mirrors
//!   real BOLT 7 (both endpoints sign + gossip). Receiver dedup
//!   (`chan_anns: HashSet<Scid>`) collapses the second cascade after
//!   first hop, and the originator-side `chan_anns` check on each
//!   node prevents two broadcasts from the originator.
//!
//! Rows whose `scid` is not in the registry, or whose `orig_node` hash
//! is not in the snapshot's NodeId set, are silently skipped and
//! counted; a one-line summary is printed at end of load.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int16Type, Int32Type, TimestampMicrosecondType, UInt64Type,
};
use arrow_array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::EventSchedule;
use crate::channels::ChannelRegistry;
use crate::message::{Direction, Gossip, GossipKind, NodeId, Scid};
use crate::topology::ln_data::{hash_pubkey, hash_scid_string};

const TYPE_CHANNEL_ANN: i16 = 1;
const TYPE_NODE_ANN: i16 = 2;
const TYPE_CHANNEL_UPD: i16 = 3;
const BATCH_ROWS: usize = 65_536;

pub struct ParquetReplay {
    /// Already-glob-expanded list of files. Read in vector order.
    pub paths: Vec<PathBuf>,
    /// Same `cfg.seed` the topology was built with — drives ID hashing.
    pub seed: u64,
    /// Snapshot's NodeId set, used to detect parquet rows whose pubkey
    /// has no matching topology vertex.
    pub snapshot_nodes: HashSet<NodeId>,
}

impl EventSchedule for ParquetReplay {
    fn build(
        &self,
        _nodes: &[NodeId],
        max: Duration,
        registry: &ChannelRegistry,
    ) -> Vec<(Duration, NodeId, Gossip)> {
        let mut out: Vec<(Duration, NodeId, Gossip)> = Vec::new();
        let mut rotor = DirectionRotor::default();
        let mut next_id: u32 = 0;
        let mut total_rows: usize = 0;
        let mut bad_scid: usize = 0;
        let mut bad_pk: usize = 0;
        let mut clamped_size: usize = 0;
        let mut anchor_us: Option<i64> = None;
        let max_us: i64 = max.as_micros().min(i64::MAX as u128) as i64;

        'files: for path in &self.paths {
            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("parquet replay: skip {} ({e})", path.display());
                    continue;
                }
            };
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap_or_else(|e| panic!("parquet open {}: {e}", path.display()))
                .with_batch_size(BATCH_ROWS);
            let reader = builder
                .build()
                .unwrap_or_else(|e| panic!("parquet build reader {}: {e}", path.display()));

            for batch_res in reader {
                let batch = batch_res
                    .unwrap_or_else(|e| panic!("parquet read batch {}: {e}", path.display()));
                let n = batch.num_rows();
                total_rows += n;

                let ts = batch
                    .column_by_name("first_seen_timestamp")
                    .expect("missing column first_seen_timestamp")
                    .as_primitive_opt::<TimestampMicrosecondType>()
                    .expect("first_seen_timestamp must be Timestamp(Microsecond, _)");
                let kinds = batch
                    .column_by_name("type")
                    .expect("missing column type")
                    .as_primitive_opt::<Int16Type>()
                    .expect("type must be Int16");
                let sizes = batch
                    .column_by_name("size")
                    .expect("missing column size")
                    .as_primitive_opt::<Int32Type>()
                    .expect("size must be Int32");
                let orig_nodes = batch
                    .column_by_name("orig_node")
                    .expect("missing column orig_node")
                    .as_string_opt::<i32>()
                    .expect("orig_node must be Utf8");
                let scids = batch
                    .column_by_name("scid")
                    .expect("missing column scid")
                    .as_primitive_opt::<UInt64Type>()
                    .expect("scid must be UInt64");

                for i in 0..n {
                    if ts.is_null(i) {
                        continue;
                    }
                    let us = ts.value(i);
                    let anchor = *anchor_us.get_or_insert(us);
                    let delay_us = us.saturating_sub(anchor);
                    if delay_us > max_us {
                        // Rows are ascending; everything after this is
                        // also out of window.
                        break 'files;
                    }
                    let delay = Duration::from_micros(delay_us as u64);

                    let kind_code = if kinds.is_null(i) { 0 } else { kinds.value(i) };
                    let raw_size = if sizes.is_null(i) { 0 } else { sizes.value(i) };
                    let size_bytes: u16 = if raw_size <= 0 {
                        0
                    } else if raw_size > u16::MAX as i32 {
                        clamped_size += 1;
                        u16::MAX
                    } else {
                        raw_size as u16
                    };

                    match kind_code {
                        TYPE_CHANNEL_UPD => {
                            if scids.is_null(i) {
                                bad_scid += 1;
                                continue;
                            }
                            let scid = hash_scid_string(self.seed, &scids.value(i).to_string());
                            if !registry.knows_scid(scid) {
                                bad_scid += 1;
                                continue;
                            }
                            let direction = rotor.next(scid);
                            let origin = registry.owner(scid, direction);
                            out.push((
                                delay,
                                origin,
                                Gossip {
                                    id: next_id,
                                    origin,
                                    kind: GossipKind::ChannelUpdate,
                                    size_bytes,
                                    scid,
                                    direction,
                                    timestamp: 0,
                                },
                            ));
                            next_id += 1;
                        }
                        TYPE_NODE_ANN => {
                            if orig_nodes.is_null(i) {
                                bad_pk += 1;
                                continue;
                            }
                            let pk = orig_nodes.value(i);
                            let origin = hash_pubkey(self.seed, pk);
                            if !self.snapshot_nodes.contains(&origin) {
                                bad_pk += 1;
                                continue;
                            }
                            out.push((
                                delay,
                                origin,
                                Gossip {
                                    id: next_id,
                                    origin,
                                    kind: GossipKind::NodeAnnouncement,
                                    size_bytes,
                                    scid: 0,
                                    direction: 0,
                                    timestamp: 0,
                                },
                            ));
                            next_id += 1;
                        }
                        TYPE_CHANNEL_ANN => {
                            if scids.is_null(i) {
                                bad_scid += 1;
                                continue;
                            }
                            let scid = hash_scid_string(self.seed, &scids.value(i).to_string());
                            if !registry.knows_scid(scid) {
                                bad_scid += 1;
                                continue;
                            }
                            for direction in [0u8, 1u8] {
                                let origin = registry.owner(scid, direction);
                                out.push((
                                    delay,
                                    origin,
                                    Gossip {
                                        id: next_id,
                                        origin,
                                        kind: GossipKind::ChannelAnnouncement,
                                        size_bytes,
                                        scid,
                                        direction: 0,
                                        timestamp: 0,
                                    },
                                ));
                                next_id += 1;
                            }
                        }
                        _ => {
                            // Unknown type code; skip silently.
                        }
                    }
                }
            }
        }

        let kept = out.len();
        let max_secs = max.as_secs();
        eprintln!(
            "parquet replay: scanned {total_rows} rows, kept {kept} \
             tuples (within {max_secs}s window), \
             skipped {bad_scid} unknown SCIDs / {bad_pk} unknown pubkeys\
             {clamp_msg}",
            clamp_msg = if clamped_size > 0 {
                format!(", clamped {clamped_size} oversized message(s) to u16::MAX")
            } else {
                String::new()
            }
        );
        out
    }
}

/// Per-SCID alternating direction picker. Stable across runs because
/// rows are processed in ascending `first_seen_timestamp` order.
#[derive(Default)]
struct DirectionRotor {
    next: HashMap<Scid, Direction>,
}

impl DirectionRotor {
    fn next(&mut self, scid: Scid) -> Direction {
        let d = self.next.entry(scid).or_insert(0);
        let out = *d;
        *d ^= 1;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_rotor_alternates_per_scid() {
        let mut r = DirectionRotor::default();
        let s1: Scid = 0xAAAA;
        let s2: Scid = 0xBBBB;
        // Per-SCID independent rotation.
        assert_eq!(r.next(s1), 0);
        assert_eq!(r.next(s1), 1);
        assert_eq!(r.next(s2), 0);
        assert_eq!(r.next(s1), 0);
        assert_eq!(r.next(s2), 1);
        assert_eq!(r.next(s1), 1);
    }
}
