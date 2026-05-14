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
//! ## Traffic reshape
//!
//! After loading, the trace is passed through a reshape pass that:
//!   1. Reassigns rows whose pubkey/SCID has no match in the topology
//!      snapshot to a random known entity of matching scope (instead
//!      of dropping them as before).
//!   2. Enforces a BOLT 7-style rate limit of 1 message per type per
//!      10-min window per entity. Excess events on a saturated home
//!      entity are first deferred to a later free window on the same
//!      entity; if the home is fully saturated, the event is
//!      reassigned to a random entity with capacity. Events that even
//!      reassignment can't fit are dropped and counted.
//!
//! Rate-limit bucket keys: `(node, kind)` for `NodeAnnouncement`;
//! `(scid, kind)` for `ChannelAnnouncement` (both endpoints emit the
//! same logical event); `(scid, direction, kind)` for `ChannelUpdate`
//! (per-endpoint per BOLT 7).
//!
//! A summary is printed at end of load, including per-kind totals,
//! deferred/reassigned/dropped counts, and the top-10 original
//! pubkey/SCID strings responsible for the excess.

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
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use super::EventSchedule;
use crate::channels::ChannelRegistry;
use crate::message::{Direction, Gossip, GossipKind, NodeId, Scid};
use crate::topology::ln_data::{hash_pubkey, hash_scid_string};

const TYPE_CHANNEL_ANN: i16 = 1;
const TYPE_NODE_ANN: i16 = 2;
const TYPE_CHANNEL_UPD: i16 = 3;
const BATCH_ROWS: usize = 65_536;

/// Width of a single rate-limit window, in seconds. BOLT 7 caps
/// `channel_update` at 1 message per direction per 10 min — we apply
/// the same cap to every (entity, kind) bucket.
const WINDOW_SECS: u64 = 600;

/// RNG salt for reshape — XORed with `cfg.seed` so the random
/// reassignment + window-jitter is deterministic across runs but
/// independent of other RNG-consuming code paths.
const RESHAPE_SEED_SALT: u64 = 0x00C0_FFEE_DEAD_BEEF_u64;

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
        // Phase A — load every parquet row as a LogicalEvent. Rows
        // whose entity is unknown become `home = Unknown` orphans
        // (Phase B reassigns them).
        let (events, load_stats) = self.load_logical(max, registry);

        // Phase B — fused reassignment + rate-limit reshape. Use at
        // least one window so very short smoke runs (< WINDOW_SECS)
        // still produce some traffic; jitter then clamps to fit.
        let num_windows = ((max.as_secs() / WINDOW_SECS) as usize).max(1);
        let (events, mut stats) = reshape(
            events,
            registry,
            &self.snapshot_nodes,
            num_windows,
            max,
            self.seed,
        );
        stats.merge_load(&load_stats);

        // Phase C — materialise tuples and log summary.
        let out = emit_tuples(&events, registry);
        stats.report(max.as_secs(), &load_stats);
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

// ---------------------------------------------------------------------------
// Phase A — load logical events from parquet
// ---------------------------------------------------------------------------

/// One parsed parquet row in pre-reshape form. `home` is `Unknown`
/// when the row's pubkey or SCID had no match in the loaded topology
/// snapshot — Phase B reassigns those.
#[derive(Debug, Clone)]
struct LogicalEvent {
    /// Time relative to the parquet anchor. Mutable: Phase B may
    /// rewrite this to a different window with fresh jitter.
    delay: Duration,
    /// Original home entity (or `Unknown` for orphans).
    home: HomeEntity,
    /// Per-kind bucket discriminator. ChannelUpdate's direction is
    /// part of the rate-limit bucket key.
    kind: LogicalKind,
    /// Carried through verbatim to the emitted `Gossip`.
    size_bytes: u16,
    /// Raw parquet pubkey hex or decimal SCID string. Used only for
    /// the top-10 excess-source report.
    orig_id_string: Box<str>,
    /// Phase B annotations — drive the reshape summary.
    was_orphan: bool,
    was_deferred: bool,
    was_reassigned: bool,
}

#[derive(Debug, Clone, Copy)]
enum HomeEntity {
    Unknown,
    Node(NodeId),
    Channel(Scid),
}

#[derive(Debug, Clone, Copy)]
enum LogicalKind {
    NodeAnnouncement,
    ChannelAnnouncement,
    ChannelUpdate { direction: Direction },
}

impl LogicalKind {
    /// Stable index into the per-kind stat arrays.
    fn kind_idx(&self) -> usize {
        match self {
            LogicalKind::NodeAnnouncement => 0,
            LogicalKind::ChannelAnnouncement => 1,
            LogicalKind::ChannelUpdate { .. } => 2,
        }
    }

    /// Which `Capacity` tracker owns this event's bucket-class.
    fn bucket_class(&self) -> BucketClass {
        match self {
            LogicalKind::NodeAnnouncement => BucketClass::NodeAnn,
            LogicalKind::ChannelAnnouncement => BucketClass::ChanAnn,
            LogicalKind::ChannelUpdate { direction: 0 } => BucketClass::ChanUpd0,
            LogicalKind::ChannelUpdate { .. } => BucketClass::ChanUpd1,
        }
    }
}

const KIND_LABELS: [&str; 3] = [
    "NodeAnnouncement",
    "ChannelAnnouncement",
    "ChannelUpdate",
];

/// Counters maintained purely by the parquet read loop.
#[derive(Default, Debug, Clone, Copy)]
struct LoadStats {
    total_rows: usize,
    clamped_size: usize,
    bad_scid_at_load: usize,
    bad_pk_at_load: usize,
    unknown_type: usize,
}

impl ParquetReplay {
    fn load_logical(
        &self,
        max: Duration,
        registry: &ChannelRegistry,
    ) -> (Vec<LogicalEvent>, LoadStats) {
        let mut out: Vec<LogicalEvent> = Vec::new();
        let mut rotor = DirectionRotor::default();
        let mut stats = LoadStats::default();
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
                stats.total_rows += n;

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
                        // also out of window. (Reshape can't extend
                        // run_duration, so dropping is the right call.)
                        break 'files;
                    }
                    let delay = Duration::from_micros(delay_us as u64);

                    let kind_code = if kinds.is_null(i) { 0 } else { kinds.value(i) };
                    let raw_size = if sizes.is_null(i) { 0 } else { sizes.value(i) };
                    let size_bytes: u16 = if raw_size <= 0 {
                        0
                    } else if raw_size > u16::MAX as i32 {
                        stats.clamped_size += 1;
                        u16::MAX
                    } else {
                        raw_size as u16
                    };

                    match kind_code {
                        TYPE_CHANNEL_UPD => {
                            if scids.is_null(i) {
                                stats.bad_scid_at_load += 1;
                                continue;
                            }
                            let raw = scids.value(i).to_string();
                            let scid = hash_scid_string(self.seed, &raw);
                            let known = registry.knows_scid(scid);
                            // Direction must be picked at load time so
                            // the per-SCID rotor stays stable across
                            // reshape runs. For orphans, the rotor
                            // still ticks (keeps determinism), then
                            // Phase B will likely reassign anyway.
                            let direction = rotor.next(scid);
                            let home = if known {
                                HomeEntity::Channel(scid)
                            } else {
                                HomeEntity::Unknown
                            };
                            out.push(LogicalEvent {
                                delay,
                                home,
                                kind: LogicalKind::ChannelUpdate { direction },
                                size_bytes,
                                orig_id_string: raw.into_boxed_str(),
                                was_orphan: !known,
                                was_deferred: false,
                                was_reassigned: false,
                            });
                        }
                        TYPE_NODE_ANN => {
                            if orig_nodes.is_null(i) {
                                stats.bad_pk_at_load += 1;
                                continue;
                            }
                            let pk = orig_nodes.value(i);
                            let origin_id = hash_pubkey(self.seed, pk);
                            let known = self.snapshot_nodes.contains(&origin_id);
                            let home = if known {
                                HomeEntity::Node(origin_id)
                            } else {
                                HomeEntity::Unknown
                            };
                            out.push(LogicalEvent {
                                delay,
                                home,
                                kind: LogicalKind::NodeAnnouncement,
                                size_bytes,
                                orig_id_string: pk.to_string().into_boxed_str(),
                                was_orphan: !known,
                                was_deferred: false,
                                was_reassigned: false,
                            });
                        }
                        TYPE_CHANNEL_ANN => {
                            if scids.is_null(i) {
                                stats.bad_scid_at_load += 1;
                                continue;
                            }
                            let raw = scids.value(i).to_string();
                            let scid = hash_scid_string(self.seed, &raw);
                            let known = registry.knows_scid(scid);
                            let home = if known {
                                HomeEntity::Channel(scid)
                            } else {
                                HomeEntity::Unknown
                            };
                            out.push(LogicalEvent {
                                delay,
                                home,
                                kind: LogicalKind::ChannelAnnouncement,
                                size_bytes,
                                orig_id_string: raw.into_boxed_str(),
                                was_orphan: !known,
                                was_deferred: false,
                                was_reassigned: false,
                            });
                        }
                        _ => {
                            stats.unknown_type += 1;
                        }
                    }
                }
            }
        }
        (out, stats)
    }
}

// ---------------------------------------------------------------------------
// Phase B — reshape: reassign orphans + enforce rate limit
// ---------------------------------------------------------------------------

/// Per-class capacity tracker. Four classes exist:
/// node-announcement-per-node, channel-announcement-per-channel,
/// channel-update-per-channel-dir-0, channel-update-per-channel-dir-1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketClass {
    NodeAnn,
    ChanAnn,
    ChanUpd0,
    ChanUpd1,
}

/// Fixed-width bitset, length = `num_windows`. Word-packed. Small
/// enough to inline (typical run: ≤ 50 windows ⇒ one u64 per entity).
#[derive(Debug, Clone)]
struct WindowBits {
    words: Vec<u64>,
    num_windows: usize,
    count: usize,
}

impl WindowBits {
    fn new(num_windows: usize) -> Self {
        let n_words = num_windows.div_ceil(64).max(1);
        Self {
            words: vec![0u64; n_words],
            num_windows,
            count: 0,
        }
    }

    #[inline]
    fn get(&self, i: usize) -> bool {
        self.words[i / 64] & (1u64 << (i % 64)) != 0
    }

    #[inline]
    fn set(&mut self, i: usize) {
        let w = i / 64;
        let mask = 1u64 << (i % 64);
        if self.words[w] & mask == 0 {
            self.words[w] |= mask;
            self.count += 1;
        }
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.count >= self.num_windows
    }

    /// Pick a uniformly-random free window. Allocates a small Vec of
    /// free indices; num_windows is bounded so cost is fine.
    fn random_free(&self, rng: &mut ChaCha8Rng) -> Option<usize> {
        if self.is_full() {
            return None;
        }
        let mut free: Vec<usize> = Vec::with_capacity(self.num_windows - self.count);
        for i in 0..self.num_windows {
            if !self.get(i) {
                free.push(i);
            }
        }
        let idx = rng.random_range(0..free.len());
        Some(free[idx])
    }
}

/// Tracks which `(entity, window)` slots are still free for one
/// bucket-class. Initialised in deterministic sorted-key order so RNG
/// draws against `has_free` are reproducible across runs.
#[derive(Debug)]
struct Capacity {
    used: HashMap<u64, WindowBits>,
    /// Entities with ≥ 1 free window. Swap-removed on saturation.
    has_free: Vec<u64>,
    has_free_pos: HashMap<u64, usize>,
    num_windows: usize,
}

impl Capacity {
    fn new(mut entities: Vec<u64>, num_windows: usize) -> Self {
        entities.sort_unstable();
        entities.dedup();
        let mut used = HashMap::with_capacity(entities.len());
        let mut has_free_pos = HashMap::with_capacity(entities.len());
        for (i, e) in entities.iter().enumerate() {
            used.insert(*e, WindowBits::new(num_windows));
            has_free_pos.insert(*e, i);
        }
        let has_free = if num_windows == 0 {
            // Degenerate: zero capacity → no entity has free room.
            entities.clear();
            Vec::new()
        } else {
            entities
        };
        // If num_windows==0, has_free_pos should also be empty.
        let has_free_pos = if num_windows == 0 {
            HashMap::new()
        } else {
            has_free_pos
        };
        Self {
            used,
            has_free,
            has_free_pos,
            num_windows,
        }
    }

    /// Try to place an event in (entity, preferred_window). If
    /// preferred is taken, fall back to any free window on the same
    /// entity. Returns `Some(window_chosen)` on success, or `None` if
    /// the entity is unknown to this class or already saturated.
    fn try_place(
        &mut self,
        entity: u64,
        preferred: usize,
        rng: &mut ChaCha8Rng,
    ) -> Option<(usize, bool)> {
        let bits = self.used.get_mut(&entity)?;
        if bits.is_full() {
            return None;
        }
        let pref = preferred.min(self.num_windows.saturating_sub(1));
        let (window, deferred) = if !bits.get(pref) {
            (pref, false)
        } else {
            let free = bits.random_free(rng)?;
            (free, true)
        };
        bits.set(window);
        if bits.is_full() {
            self.evict(entity);
        }
        Some((window, deferred))
    }

    fn random_entity(&self, rng: &mut ChaCha8Rng) -> Option<u64> {
        if self.has_free.is_empty() {
            None
        } else {
            let idx = rng.random_range(0..self.has_free.len());
            Some(self.has_free[idx])
        }
    }

    fn evict(&mut self, entity: u64) {
        let Some(pos) = self.has_free_pos.remove(&entity) else {
            return;
        };
        self.has_free.swap_remove(pos);
        if pos < self.has_free.len() {
            let swapped_in = self.has_free[pos];
            self.has_free_pos.insert(swapped_in, pos);
        }
    }
}

#[derive(Default, Debug)]
struct ReshapeStats {
    /// Surviving events per kind (NodeAnn / ChanAnn / ChanUpd).
    total_by_kind: [usize; 3],
    /// Moved to a different window on the same entity.
    deferred_by_kind: [usize; 3],
    /// Moved to a different entity (orphans OR rate-limit overflows
    /// from a saturated home). Counts BOTH reasons together; the
    /// orphan-only sub-count is in `orphan_by_kind`.
    reassigned_by_kind: [usize; 3],
    /// Subset of reassigned events that started as `home = Unknown`.
    orphan_by_kind: [usize; 3],
    /// Events that even reassignment couldn't fit (class fully
    /// saturated) — discarded.
    dropped_by_kind: [usize; 3],
    /// Per-kind, original parquet id-string → count of "adjusted"
    /// events (deferred OR reassigned OR orphan). Drives the top-10
    /// excess-source report.
    excess_by_kind: [HashMap<Box<str>, usize>; 3],
}

impl ReshapeStats {
    fn merge_load(&mut self, _load: &LoadStats) {
        // Load-only stats are reported alongside reshape stats but
        // not folded into per-kind counters.
    }

    fn report(&self, run_secs: u64, load: &LoadStats) {
        eprintln!(
            "parquet replay: scanned {total} rows{clamp}{badscid}{badpk}{badty} \
             (window={run_secs}s)",
            total = load.total_rows,
            clamp = if load.clamped_size > 0 {
                format!(", clamped {} oversized size(s)", load.clamped_size)
            } else {
                String::new()
            },
            badscid = if load.bad_scid_at_load > 0 {
                format!(", {} rows with null SCID dropped", load.bad_scid_at_load)
            } else {
                String::new()
            },
            badpk = if load.bad_pk_at_load > 0 {
                format!(", {} rows with null pubkey dropped", load.bad_pk_at_load)
            } else {
                String::new()
            },
            badty = if load.unknown_type > 0 {
                format!(", {} rows with unknown type dropped", load.unknown_type)
            } else {
                String::new()
            },
        );
        for k in 0..3 {
            eprintln!(
                "parquet reshape: {label}: total={total} deferred={def} \
                 reassigned={reass} orphans={orph} dropped={drop}",
                label = KIND_LABELS[k],
                total = self.total_by_kind[k],
                def = self.deferred_by_kind[k],
                reass = self.reassigned_by_kind[k],
                orph = self.orphan_by_kind[k],
                drop = self.dropped_by_kind[k],
            );
        }
        for k in 0..3 {
            if self.excess_by_kind[k].is_empty() {
                continue;
            }
            let mut top: Vec<(&Box<str>, &usize)> = self.excess_by_kind[k].iter().collect();
            top.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
            let n = top.len().min(10);
            let joined: Vec<String> = top
                .iter()
                .take(n)
                .map(|(id, c)| format!("{}={}", id, c))
                .collect();
            eprintln!(
                "parquet reshape: top-{n} {label} excess sources: {list}",
                label = KIND_LABELS[k],
                list = joined.join(", "),
            );
        }
        let any_dropped: usize = self.dropped_by_kind.iter().sum();
        if any_dropped > 0 {
            eprintln!(
                "parquet reshape: WARNING {any_dropped} events dropped \
                 (total input > num_entities × num_windows for some \
                 bucket class). Consider increasing run_duration or \
                 reducing the parquet input."
            );
        }
    }
}

/// Phase B implementation. Fused single-pass reshape:
/// 1. For each event, identify the home entity (or pick a random one
///    if it was an orphan).
/// 2. Try the preferred 10-min window on the home entity.
/// 3. If the home entity is saturated, pick a random entity in the
///    same bucket-class that still has capacity and place there.
/// 4. If the entire class is saturated, drop and count.
fn reshape(
    events: Vec<LogicalEvent>,
    registry: &ChannelRegistry,
    snapshot_nodes: &HashSet<NodeId>,
    num_windows: usize,
    max_duration: Duration,
    seed: u64,
) -> (Vec<LogicalEvent>, ReshapeStats) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ RESHAPE_SEED_SALT);

    // Build the four capacity trackers. Sorted-key construction makes
    // RNG-driven entity picks reproducible across runs at fixed seed.
    let mut node_cap = Capacity::new(
        snapshot_nodes.iter().copied().collect::<Vec<NodeId>>(),
        num_windows,
    );
    let scid_vec: Vec<Scid> = registry.scids().to_vec();
    let mut chan_ann_cap = Capacity::new(scid_vec.clone(), num_windows);
    let mut chan_upd0_cap = Capacity::new(scid_vec.clone(), num_windows);
    let mut chan_upd1_cap = Capacity::new(scid_vec, num_windows);

    let mut stats = ReshapeStats::default();
    let mut surviving: Vec<LogicalEvent> = Vec::with_capacity(events.len());

    for mut ev in events {
        let class = ev.kind.bucket_class();
        let cap: &mut Capacity = match class {
            BucketClass::NodeAnn => &mut node_cap,
            BucketClass::ChanAnn => &mut chan_ann_cap,
            BucketClass::ChanUpd0 => &mut chan_upd0_cap,
            BucketClass::ChanUpd1 => &mut chan_upd1_cap,
        };

        // Pick home entity. Orphans take a random in-class entity now;
        // any subsequent reassignment for rate-limit reasons still
        // applies on top.
        let preferred_window = (ev.delay.as_secs() / WINDOW_SECS) as usize;

        let mut home_entity: u64 = match ev.home {
            HomeEntity::Node(n) => n,
            HomeEntity::Channel(s) => s,
            HomeEntity::Unknown => {
                let Some(picked) = cap.random_entity(&mut rng) else {
                    stats.dropped_by_kind[ev.kind.kind_idx()] += 1;
                    continue;
                };
                picked
            }
        };

        let kind_idx = ev.kind.kind_idx();

        // Orphans were placed on a randomly-picked entity above, so
        // they always count as reassigned even if the placement
        // succeeds on the first try.
        let mut reassigned = ev.was_orphan;
        // Try the home entity first.
        let mut placement = cap.try_place(home_entity, preferred_window, &mut rng);

        // If the home entity is saturated, jump to a fresh random
        // entity. Loop because a freshly-picked entity could (in
        // theory) be saturated by an earlier swap-remove race; in
        // practice `random_entity` returns from `has_free`, so it's a
        // one-shot. The loop guards against future invariant changes.
        while placement.is_none() {
            let Some(picked) = cap.random_entity(&mut rng) else {
                // Class fully saturated.
                stats.dropped_by_kind[kind_idx] += 1;
                break;
            };
            home_entity = picked;
            reassigned = true;
            placement = cap.try_place(home_entity, preferred_window, &mut rng);
        }

        let Some((window, deferred)) = placement else {
            continue; // already counted under dropped
        };

        // Tally stats. "Deferred" implies same entity, different window.
        // "Reassigned" implies different entity (the new entity may
        // place the event in its preferred or a deferred window — we
        // do NOT additionally count it as deferred in that case).
        let orig_id = ev.orig_id_string.clone();
        let was_orphan = ev.was_orphan;
        if reassigned {
            ev.was_reassigned = true;
            stats.reassigned_by_kind[kind_idx] += 1;
            if was_orphan {
                stats.orphan_by_kind[kind_idx] += 1;
            }
        } else if deferred {
            ev.was_deferred = true;
            stats.deferred_by_kind[kind_idx] += 1;
        }

        // Set the new home + jittered delay. Jitter is bounded to the
        // intersection of [window, window+1) * WINDOW_SECS and
        // [0, max_duration] so we never emit an event past the sim
        // deadline (which would silently get dropped by the executor).
        ev.home = match class {
            BucketClass::NodeAnn => HomeEntity::Node(home_entity),
            _ => HomeEntity::Channel(home_entity),
        };
        let window_start_secs = window as u64 * WINDOW_SECS;
        let window_end_secs = (window_start_secs + WINDOW_SECS).min(max_duration.as_secs());
        let window_width_us = window_end_secs
            .saturating_sub(window_start_secs)
            .saturating_mul(1_000_000);
        let jitter_us = if window_width_us > 0 {
            rng.random_range(0..window_width_us)
        } else {
            0
        };
        ev.delay =
            Duration::from_secs(window_start_secs) + Duration::from_micros(jitter_us);

        // Top-10 excess sources: any "adjusted" event contributes
        // (deferred, reassigned, or orphan-reassigned).
        if ev.was_deferred || ev.was_reassigned || was_orphan {
            *stats.excess_by_kind[kind_idx]
                .entry(orig_id)
                .or_insert(0) += 1;
        }
        stats.total_by_kind[kind_idx] += 1;

        surviving.push(ev);
    }

    (surviving, stats)
}

// ---------------------------------------------------------------------------
// Phase C — materialise LogicalEvent → (delay, originator, Gossip)
// ---------------------------------------------------------------------------

fn emit_tuples(
    events: &[LogicalEvent],
    registry: &ChannelRegistry,
) -> Vec<(Duration, NodeId, Gossip)> {
    let mut out: Vec<(Duration, NodeId, Gossip)> = Vec::with_capacity(events.len());
    for ev in events {
        match ev.kind {
            LogicalKind::NodeAnnouncement => {
                let HomeEntity::Node(origin) = ev.home else {
                    // Reshape always resolves to Node for NodeAnn.
                    continue;
                };
                out.push((
                    ev.delay,
                    origin,
                    Gossip {
                        id: 0,
                        origin: Some(origin),
                        kind: GossipKind::NodeAnnouncement,
                        size_bytes: ev.size_bytes,
                        scid: None,
                        direction: 0,
                        timestamp: 0,
                    },
                ));
            }
            LogicalKind::ChannelUpdate { direction } => {
                let HomeEntity::Channel(scid) = ev.home else {
                    continue;
                };
                let origin_route = registry.owner(scid, direction);
                out.push((
                    ev.delay,
                    origin_route,
                    Gossip {
                        id: 0,
                        origin: None,
                        kind: GossipKind::ChannelUpdate,
                        size_bytes: ev.size_bytes,
                        scid: Some(scid),
                        direction,
                        timestamp: 0,
                    },
                ));
            }
            LogicalKind::ChannelAnnouncement => {
                let HomeEntity::Channel(scid) = ev.home else {
                    continue;
                };
                // Emit TWO tuples at the same delay (one per
                // endpoint), matching real BOLT 7. Receiver-side
                // dedup (`chan_anns: HashSet<Scid>`) collapses the
                // second cascade. Note: the rate-limit bucket already
                // accounted for this as a SINGLE logical event.
                for direction in [0u8, 1u8] {
                    let origin_route = registry.owner(scid, direction);
                    out.push((
                        ev.delay,
                        origin_route,
                        Gossip {
                            id: 0,
                            origin: None,
                            kind: GossipKind::ChannelAnnouncement,
                            size_bytes: ev.size_bytes,
                            scid: Some(scid),
                            direction: 0,
                            timestamp: 0,
                        },
                    ));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::graph::{NodeAlgo, Topology};

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

    fn ev_node_ann(delay_secs: u64, home: HomeEntity, was_orphan: bool) -> LogicalEvent {
        LogicalEvent {
            delay: Duration::from_secs(delay_secs),
            home,
            kind: LogicalKind::NodeAnnouncement,
            size_bytes: 100,
            orig_id_string: "test_pk".into(),
            was_orphan,
            was_deferred: false,
            was_reassigned: false,
        }
    }

    /// Empty channel registry. Some tests don't care about channels.
    fn empty_registry() -> (Topology, ChannelRegistry) {
        let mut topo = Topology::empty(0, NodeAlgo::Flooding);
        let reg = ChannelRegistry::from_iter(&mut topo, std::iter::empty());
        (topo, reg)
    }

    #[test]
    fn orphan_node_ann_gets_reassigned_to_known_node() {
        let mut snapshot = HashSet::new();
        let known: NodeId = 42;
        snapshot.insert(known);
        let (_topo, reg) = empty_registry();

        let events = vec![ev_node_ann(0, HomeEntity::Unknown, true)];
        let (out, stats) = reshape(events, &reg, &snapshot, 6, Duration::from_secs(6 as u64 * 600), 0xC0DE);

        assert_eq!(out.len(), 1, "orphan should be placed, not dropped");
        match out[0].home {
            HomeEntity::Node(n) => assert_eq!(n, known),
            _ => panic!("expected Node home, got {:?}", out[0].home),
        }
        assert!(out[0].was_reassigned, "orphan must be flagged reassigned");
        assert_eq!(stats.orphan_by_kind[0], 1);
        assert_eq!(stats.reassigned_by_kind[0], 1);
        assert_eq!(stats.dropped_by_kind[0], 0);
    }

    #[test]
    fn second_node_ann_same_window_is_deferred() {
        let mut snapshot = HashSet::new();
        let n: NodeId = 7;
        snapshot.insert(n);
        let (_topo, reg) = empty_registry();

        // Both events fall in window 0 (delay < 600s).
        let events = vec![
            ev_node_ann(10, HomeEntity::Node(n), false),
            ev_node_ann(50, HomeEntity::Node(n), false),
        ];
        let (out, stats) = reshape(events, &reg, &snapshot, 6, Duration::from_secs(6 as u64 * 600), 0xC0DE);

        assert_eq!(out.len(), 2);
        // Both events stay on the same node (no reassignment) but in
        // distinct windows.
        for e in &out {
            assert!(matches!(e.home, HomeEntity::Node(x) if x == n));
        }
        let w0 = (out[0].delay.as_secs() / WINDOW_SECS) as usize;
        let w1 = (out[1].delay.as_secs() / WINDOW_SECS) as usize;
        assert_ne!(w0, w1, "second event must move to a different window");
        assert_eq!(stats.deferred_by_kind[0], 1);
        assert_eq!(stats.reassigned_by_kind[0], 0);
    }

    #[test]
    fn saturated_home_reassigns_to_other_entity() {
        // num_windows = 1, two known nodes. Two NodeAnns on node A —
        // the second can't fit on A, must reassign to B.
        let mut snapshot = HashSet::new();
        let a: NodeId = 1;
        let b: NodeId = 2;
        snapshot.insert(a);
        snapshot.insert(b);
        let (_topo, reg) = empty_registry();

        let events = vec![
            ev_node_ann(0, HomeEntity::Node(a), false),
            ev_node_ann(0, HomeEntity::Node(a), false),
        ];
        let (out, stats) = reshape(events, &reg, &snapshot, 1, Duration::from_secs(1 as u64 * 600), 0xC0DE);

        assert_eq!(out.len(), 2);
        // First event lands on A. Second event must end up on B.
        assert!(matches!(out[0].home, HomeEntity::Node(x) if x == a));
        assert!(matches!(out[1].home, HomeEntity::Node(x) if x == b));
        assert_eq!(stats.reassigned_by_kind[0], 1);
        assert_eq!(stats.dropped_by_kind[0], 0);
    }

    #[test]
    fn fully_saturated_class_drops_events() {
        // num_windows = 1, one known node. Two NodeAnns on that node
        // — second has nowhere to go.
        let mut snapshot = HashSet::new();
        let n: NodeId = 9;
        snapshot.insert(n);
        let (_topo, reg) = empty_registry();

        let events = vec![
            ev_node_ann(0, HomeEntity::Node(n), false),
            ev_node_ann(0, HomeEntity::Node(n), false),
        ];
        let (out, stats) = reshape(events, &reg, &snapshot, 1, Duration::from_secs(1 as u64 * 600), 0xC0DE);

        assert_eq!(out.len(), 1, "second event must be dropped");
        assert_eq!(stats.dropped_by_kind[0], 1);
        assert_eq!(stats.total_by_kind[0], 1);
    }

    #[test]
    fn reshape_is_deterministic_at_fixed_seed() {
        let mut snapshot = HashSet::new();
        for i in 1..=8u64 {
            snapshot.insert(i);
        }
        let (_topo, reg) = empty_registry();
        let events = vec![
            ev_node_ann(0, HomeEntity::Unknown, true),
            ev_node_ann(700, HomeEntity::Node(1), false),
            ev_node_ann(700, HomeEntity::Node(1), false),
            ev_node_ann(1300, HomeEntity::Unknown, true),
        ];

        let (out_a, _) = reshape(events.clone(), &reg, &snapshot, 4, Duration::from_secs(2400), 0xC0DE);
        let (out_b, _) = reshape(events, &reg, &snapshot, 4, Duration::from_secs(4 as u64 * 600), 0xC0DE);

        assert_eq!(out_a.len(), out_b.len());
        for (a, b) in out_a.iter().zip(out_b.iter()) {
            // Both delays and homes must match exactly.
            let home_a = match a.home {
                HomeEntity::Node(x) => x,
                HomeEntity::Channel(x) => x,
                HomeEntity::Unknown => panic!("unresolved orphan"),
            };
            let home_b = match b.home {
                HomeEntity::Node(x) => x,
                HomeEntity::Channel(x) => x,
                HomeEntity::Unknown => panic!("unresolved orphan"),
            };
            assert_eq!(home_a, home_b);
            assert_eq!(a.delay, b.delay);
        }
    }

    #[test]
    fn window_bits_packs_and_finds_free_slots() {
        let mut wb = WindowBits::new(6);
        assert!(!wb.get(0));
        wb.set(0);
        assert!(wb.get(0));
        assert_eq!(wb.count, 1);
        // Idempotent set.
        wb.set(0);
        assert_eq!(wb.count, 1);

        let mut rng = ChaCha8Rng::seed_from_u64(1);
        // 5 free slots remaining; random_free returns one of {1..5}.
        let free = wb.random_free(&mut rng).unwrap();
        assert!((1..6).contains(&free));
    }

    #[test]
    fn capacity_evicts_on_saturation() {
        let mut cap = Capacity::new(vec![1u64, 2u64], 2);
        assert_eq!(cap.has_free.len(), 2);
        let mut rng = ChaCha8Rng::seed_from_u64(0);

        // Fill entity 1.
        cap.try_place(1, 0, &mut rng).unwrap();
        cap.try_place(1, 1, &mut rng).unwrap();
        assert_eq!(cap.has_free.len(), 1, "saturated entity must be evicted");
        // try_place on a saturated entity returns None.
        assert!(cap.try_place(1, 0, &mut rng).is_none());
    }
}
