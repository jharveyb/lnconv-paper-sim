//! Background Parquet writer multiplex — one OS thread per file.
//!
//! Six logical row kinds, each going to its own Parquet file sharing
//! a common `<tag>` suffix:
//!
//! | file                                  | one row per                                |
//! |---------------------------------------|--------------------------------------------|
//! | `<tag>-msg_stats.parquet`             | finalised message                          |
//! | `<tag>-node_counters.parquet`         | (node, flush_event) snapshot               |
//! | `<tag>-node_reservoirs.parquet`       | (node, kind, reservoir_sample)             |
//! | `<tag>-overflow_events.parquet`       | overflow event                             |
//! | `<tag>-run_meta.parquet`              | single row, written once at sim init       |
//! | `<tag>-node_pubkey.parquet`           | node, FromCsv runs only                    |
//!
//! All row types implement the [`WriteRow`] trait, which exposes a
//! schema + a single `to_batch()` that uses Arrow's typed builders to
//! avoid the per-column `collect::<Vec<_>>()` intermediates.
//!
//! Each output file gets its own bounded `crossbeam_channel` and its
//! own writer OS thread. Arrow encoding + ZSTD compression therefore
//! run in parallel across files instead of being serialised behind a
//! single multiplex thread — wall clock collapses from
//! `Σ per-file encode time` to `max(per-file encode time)`. Hot files
//! (`msg_stats`, `overflow_events`, `node_counters`) use `ZstdLevel(1)`
//! for ~2× compression throughput; the cold files keep the default
//! level. Send failures `panic!` — the writer crashing is a hard error,
//! not silent corruption.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use arrow_array::builder::{
    Float64Builder, StringBuilder, UInt8Builder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use crossbeam_channel::{Receiver, Sender, bounded};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::message::{NodeId, NodeIdx};
use crate::metrics::{MsgStats, NodeCounters, OverflowEvent};

/// Per-writer buffer flush threshold. Each typed `Stream<R>` buffers
/// up to this many rows before encoding them into an Arrow
/// `RecordBatch` and handing them to the underlying `ArrowWriter`.
/// Doubled from the previous 65 536 — peak per-stream buffer ~22 MB,
/// total peak across 6 streams ~140 MB. Trade is fewer encode calls
/// in exchange for slightly more RAM.
const FLUSH_EVERY: usize = 131_072;

/// Per-file channel depth. Bounded so backpressure surfaces as
/// wall-time, not RAM growth. With one channel per output file a
/// stalled writer only blocks its own producer code path.
const PER_STREAM_DEPTH: usize = 256_000;

/// Output multiplex sender — cloneable. Holds one
/// `crossbeam_channel::Sender` per output Parquet file; `send` does a
/// single-branch enum dispatch into the right channel. Each channel
/// is drained by its own writer thread, so encode + compress for
/// different files runs in parallel instead of being serialised
/// behind one multiplex thread.
#[derive(Clone)]
pub struct RowSender {
    msg_stats: Sender<MsgStats>,
    node_counters: Sender<NodeCountersRow>,
    node_reservoirs: Sender<NodeReservoirRow>,
    overflow_events: Sender<OverflowEventRow>,
    run_meta: Sender<RunMetaRow>,
    node_pubkey: Sender<NodePubkeyRow>,
}

impl RowSender {
    /// Single-branch dispatch into the per-file channel. Panics on
    /// disconnect — the writer thread crashing is a hard error, not
    /// silent corruption (see module docs).
    pub fn send(&self, row: WriterRow) {
        let r = match row {
            WriterRow::MsgStats(s) => self.msg_stats.send(s).map_err(|e| WriterRow::MsgStats(e.0)),
            WriterRow::NodeCounters(r) => self
                .node_counters
                .send(r)
                .map_err(|e| WriterRow::NodeCounters(e.0)),
            WriterRow::NodeReservoir(r) => self
                .node_reservoirs
                .send(r)
                .map_err(|e| WriterRow::NodeReservoir(e.0)),
            WriterRow::OverflowEvent(r) => self
                .overflow_events
                .send(r)
                .map_err(|e| WriterRow::OverflowEvent(e.0)),
            WriterRow::RunMeta(r) => self.run_meta.send(r).map_err(|e| WriterRow::RunMeta(e.0)),
            WriterRow::NodePubkey(r) => self
                .node_pubkey
                .send(r)
                .map_err(|e| WriterRow::NodePubkey(e.0)),
        };
        r.expect("stats writer channel closed before sim end");
    }
}

/// All row variants the writer can route.
#[derive(Debug)]
pub enum WriterRow {
    MsgStats(MsgStats),
    NodeCounters(NodeCountersRow),
    NodeReservoir(NodeReservoirRow),
    OverflowEvent(OverflowEventRow),
    RunMeta(RunMetaRow),
    NodePubkey(NodePubkeyRow),
}

// ---- row types ------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct NodeCountersRow {
    pub node_idx: NodeIdx,
    pub time_ns: u64,
    pub bytes_in_sketch: u64,
    pub bytes_out_sketch: u64,
    pub bytes_in_gossip: u64,
    pub bytes_out_gossip: u64,
    pub duplicates: u64,
    pub duplicates_bytes: u64,
    pub sketches_sent: u64,
    pub sketches_received: u64,
    pub overflowed_chan_updates: u64,
    pub overflowed_node_anns: u64,
    pub overflowed_chan_anns: u64,
    pub chan_updates_intersection: u64,
    pub chan_updates_a_only: u64,
    pub chan_updates_b_only: u64,
    pub node_anns_intersection: u64,
    pub node_anns_a_only: u64,
    pub node_anns_b_only: u64,
    pub chan_anns_intersection: u64,
    pub chan_anns_a_only: u64,
    pub chan_anns_b_only: u64,
}

impl NodeCountersRow {
    pub fn from_counters(node_idx: NodeIdx, time_ns: u64, c: &NodeCounters) -> Self {
        Self {
            node_idx,
            time_ns,
            bytes_in_sketch: c.bytes_in_sketch,
            bytes_out_sketch: c.bytes_out_sketch,
            bytes_in_gossip: c.bytes_in_gossip,
            bytes_out_gossip: c.bytes_out_gossip,
            duplicates: c.duplicates,
            duplicates_bytes: c.duplicates_bytes,
            sketches_sent: c.sketches_sent,
            sketches_received: c.sketches_received,
            overflowed_chan_updates: c.overflowed_chan_updates,
            overflowed_node_anns: c.overflowed_node_anns,
            overflowed_chan_anns: c.overflowed_chan_anns,
            chan_updates_intersection: c.chan_updates.intersection,
            chan_updates_a_only: c.chan_updates.a_only,
            chan_updates_b_only: c.chan_updates.b_only,
            node_anns_intersection: c.node_anns.intersection,
            node_anns_a_only: c.node_anns.a_only,
            node_anns_b_only: c.node_anns.b_only,
            chan_anns_intersection: c.chan_anns.intersection,
            chan_anns_a_only: c.chan_anns.a_only,
            chan_anns_b_only: c.chan_anns.b_only,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NodeReservoirRow {
    pub node_idx: NodeIdx,
    /// `0 = chan_updates`, `1 = node_anns`, `2 = chan_anns`.
    pub kind: u8,
    pub intersection: u32,
    pub a_only: u32,
    pub b_only: u32,
    pub total_seen: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct OverflowEventRow {
    pub time_ns: u64,
    pub receiver_idx: NodeIdx,
    pub peer_id: NodeId,
    pub kind: u8,
    pub amount: u32,
    pub total_diff: u32,
}

impl OverflowEventRow {
    pub fn from_event(e: &OverflowEvent) -> Self {
        Self {
            time_ns: e.time_ns,
            receiver_idx: e.receiver_idx,
            peer_id: e.peer_id,
            kind: e.kind.as_u8(),
            amount: e.amount,
            total_diff: e.total_diff,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunMetaRow {
    pub seed: u64,
    pub n_nodes: u64,
    pub mean_degree: f64,
    pub min_degree: u64,
    pub max_degree: u64,
    pub diameter: u64,
    pub mean_path_length: f64,
    pub stagger_secs: f64,
    pub capacity_chan_updates: u64,
    pub capacity_node_anns: u64,
    pub capacity_chan_anns: u64,
    pub events_chan_update: u64,
    pub events_node_ann: u64,
    pub events_chan_ann: u64,
    pub duration_seconds: u64,
    pub predicted_p50_secs: f64,
    pub predicted_p99_secs: f64,
    pub predicted_p100_secs: f64,
    pub algo: String,
    pub topology_kind: String,
    pub event_kind: String,
}

/// Inputs to [`RunMetaRow::new`]. Groups the integer config fields so
/// the constructor signature stays under control.
pub struct RunMetaInputs<'a> {
    pub seed: u64,
    pub duration_seconds: u64,
    pub algo: &'a str,
    pub topology_kind: &'a str,
    pub event_kind: &'a str,
    pub n_nodes: u64,
    pub mean_degree: f64,
    pub min_degree: u64,
    pub max_degree: u64,
    pub diameter: u64,
    pub mean_path_length: f64,
    pub capacity_chan_updates: u64,
    pub capacity_node_anns: u64,
    pub capacity_chan_anns: u64,
    pub events_chan_update: u64,
    pub events_node_ann: u64,
    pub events_chan_ann: u64,
}

impl RunMetaRow {
    /// Build a row from grouped inputs + optional sketch predictions.
    /// `None` predictions encode "non-sketch run" structurally — the
    /// `stagger_secs` / `predicted_*` fields land as 0.0 in the
    /// Parquet, and the DuckDB report's `print_run_meta` only renders
    /// the sketch block when `stagger_secs > 0`.
    pub fn new(
        inputs: RunMetaInputs<'_>,
        predictions: Option<crate::spread_model::SketchPredictions>,
    ) -> Self {
        let (stagger_secs, p50, p99, p100) = match predictions {
            Some(p) => (p.stagger_secs, p.p50_secs, p.p99_secs, p.p100_secs),
            None => (0.0, 0.0, 0.0, 0.0),
        };
        Self {
            seed: inputs.seed,
            n_nodes: inputs.n_nodes,
            mean_degree: inputs.mean_degree,
            min_degree: inputs.min_degree,
            max_degree: inputs.max_degree,
            diameter: inputs.diameter,
            mean_path_length: inputs.mean_path_length,
            stagger_secs,
            capacity_chan_updates: inputs.capacity_chan_updates,
            capacity_node_anns: inputs.capacity_node_anns,
            capacity_chan_anns: inputs.capacity_chan_anns,
            events_chan_update: inputs.events_chan_update,
            events_node_ann: inputs.events_node_ann,
            events_chan_ann: inputs.events_chan_ann,
            duration_seconds: inputs.duration_seconds,
            predicted_p50_secs: p50,
            predicted_p99_secs: p99,
            predicted_p100_secs: p100,
            algo: inputs.algo.to_string(),
            topology_kind: inputs.topology_kind.to_string(),
            event_kind: inputs.event_kind.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodePubkeyRow {
    pub node_idx: NodeIdx,
    pub node_id: NodeId,
    pub pubkey: String,
}

impl NodePubkeyRow {
    pub fn new(node_idx: NodeIdx, node_id: NodeId, pubkey: String) -> Self {
        Self { node_idx, node_id, pubkey }
    }
}

// ---- WriteRow trait + per-type impls --------------------------------

/// A row that knows its own schema and how to build a `RecordBatch`
/// from a slice of self. Lets the writer thread dispatch buffered
/// flushes generically.
pub trait WriteRow: Sized {
    fn schema() -> Arc<Schema>;
    fn to_batch(rows: &[Self]) -> RecordBatch;
}

impl WriteRow for NodeCountersRow {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("node_idx", DataType::UInt32, false),
            Field::new("time_ns", DataType::UInt64, false),
            Field::new("bytes_in_sketch", DataType::UInt64, false),
            Field::new("bytes_out_sketch", DataType::UInt64, false),
            Field::new("bytes_in_gossip", DataType::UInt64, false),
            Field::new("bytes_out_gossip", DataType::UInt64, false),
            Field::new("duplicates", DataType::UInt64, false),
            Field::new("duplicates_bytes", DataType::UInt64, false),
            Field::new("sketches_sent", DataType::UInt64, false),
            Field::new("sketches_received", DataType::UInt64, false),
            Field::new("overflowed_chan_updates", DataType::UInt64, false),
            Field::new("overflowed_node_anns", DataType::UInt64, false),
            Field::new("overflowed_chan_anns", DataType::UInt64, false),
            Field::new("chan_updates_intersection", DataType::UInt64, false),
            Field::new("chan_updates_a_only", DataType::UInt64, false),
            Field::new("chan_updates_b_only", DataType::UInt64, false),
            Field::new("node_anns_intersection", DataType::UInt64, false),
            Field::new("node_anns_a_only", DataType::UInt64, false),
            Field::new("node_anns_b_only", DataType::UInt64, false),
            Field::new("chan_anns_intersection", DataType::UInt64, false),
            Field::new("chan_anns_a_only", DataType::UInt64, false),
            Field::new("chan_anns_b_only", DataType::UInt64, false),
        ]))
    }

    fn to_batch(rows: &[Self]) -> RecordBatch {
        let n = rows.len();
        // Each builder appends directly into its own typed buffer —
        // no intermediate Vec, no per-column `collect::<Vec<_>>`.
        let mut node_idx = UInt32Builder::with_capacity(n);
        let mut time_ns = UInt64Builder::with_capacity(n);
        let mut bytes_in_sketch = UInt64Builder::with_capacity(n);
        let mut bytes_out_sketch = UInt64Builder::with_capacity(n);
        let mut bytes_in_gossip = UInt64Builder::with_capacity(n);
        let mut bytes_out_gossip = UInt64Builder::with_capacity(n);
        let mut duplicates = UInt64Builder::with_capacity(n);
        let mut duplicates_bytes = UInt64Builder::with_capacity(n);
        let mut sketches_sent = UInt64Builder::with_capacity(n);
        let mut sketches_received = UInt64Builder::with_capacity(n);
        let mut o_cu = UInt64Builder::with_capacity(n);
        let mut o_na = UInt64Builder::with_capacity(n);
        let mut o_ca = UInt64Builder::with_capacity(n);
        let mut cu_i = UInt64Builder::with_capacity(n);
        let mut cu_a = UInt64Builder::with_capacity(n);
        let mut cu_b = UInt64Builder::with_capacity(n);
        let mut na_i = UInt64Builder::with_capacity(n);
        let mut na_a = UInt64Builder::with_capacity(n);
        let mut na_b = UInt64Builder::with_capacity(n);
        let mut ca_i = UInt64Builder::with_capacity(n);
        let mut ca_a = UInt64Builder::with_capacity(n);
        let mut ca_b = UInt64Builder::with_capacity(n);
        for r in rows {
            node_idx.append_value(r.node_idx);
            time_ns.append_value(r.time_ns);
            bytes_in_sketch.append_value(r.bytes_in_sketch);
            bytes_out_sketch.append_value(r.bytes_out_sketch);
            bytes_in_gossip.append_value(r.bytes_in_gossip);
            bytes_out_gossip.append_value(r.bytes_out_gossip);
            duplicates.append_value(r.duplicates);
            duplicates_bytes.append_value(r.duplicates_bytes);
            sketches_sent.append_value(r.sketches_sent);
            sketches_received.append_value(r.sketches_received);
            o_cu.append_value(r.overflowed_chan_updates);
            o_na.append_value(r.overflowed_node_anns);
            o_ca.append_value(r.overflowed_chan_anns);
            cu_i.append_value(r.chan_updates_intersection);
            cu_a.append_value(r.chan_updates_a_only);
            cu_b.append_value(r.chan_updates_b_only);
            na_i.append_value(r.node_anns_intersection);
            na_a.append_value(r.node_anns_a_only);
            na_b.append_value(r.node_anns_b_only);
            ca_i.append_value(r.chan_anns_intersection);
            ca_a.append_value(r.chan_anns_a_only);
            ca_b.append_value(r.chan_anns_b_only);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(node_idx.finish()),
            Arc::new(time_ns.finish()),
            Arc::new(bytes_in_sketch.finish()),
            Arc::new(bytes_out_sketch.finish()),
            Arc::new(bytes_in_gossip.finish()),
            Arc::new(bytes_out_gossip.finish()),
            Arc::new(duplicates.finish()),
            Arc::new(duplicates_bytes.finish()),
            Arc::new(sketches_sent.finish()),
            Arc::new(sketches_received.finish()),
            Arc::new(o_cu.finish()),
            Arc::new(o_na.finish()),
            Arc::new(o_ca.finish()),
            Arc::new(cu_i.finish()),
            Arc::new(cu_a.finish()),
            Arc::new(cu_b.finish()),
            Arc::new(na_i.finish()),
            Arc::new(na_a.finish()),
            Arc::new(na_b.finish()),
            Arc::new(ca_i.finish()),
            Arc::new(ca_a.finish()),
            Arc::new(ca_b.finish()),
        ];
        RecordBatch::try_new(Self::schema(), columns).expect("NodeCountersRow batch")
    }
}

impl WriteRow for NodeReservoirRow {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("node_idx", DataType::UInt32, false),
            Field::new("kind", DataType::UInt8, false),
            Field::new("intersection", DataType::UInt32, false),
            Field::new("a_only", DataType::UInt32, false),
            Field::new("b_only", DataType::UInt32, false),
            Field::new("total_seen", DataType::UInt64, false),
        ]))
    }

    fn to_batch(rows: &[Self]) -> RecordBatch {
        let n = rows.len();
        let mut node_idx = UInt32Builder::with_capacity(n);
        let mut kind = UInt8Builder::with_capacity(n);
        let mut intersection = UInt32Builder::with_capacity(n);
        let mut a_only = UInt32Builder::with_capacity(n);
        let mut b_only = UInt32Builder::with_capacity(n);
        let mut total_seen = UInt64Builder::with_capacity(n);
        for r in rows {
            node_idx.append_value(r.node_idx);
            kind.append_value(r.kind);
            intersection.append_value(r.intersection);
            a_only.append_value(r.a_only);
            b_only.append_value(r.b_only);
            total_seen.append_value(r.total_seen);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(node_idx.finish()),
            Arc::new(kind.finish()),
            Arc::new(intersection.finish()),
            Arc::new(a_only.finish()),
            Arc::new(b_only.finish()),
            Arc::new(total_seen.finish()),
        ];
        RecordBatch::try_new(Self::schema(), columns).expect("NodeReservoirRow batch")
    }
}

impl WriteRow for OverflowEventRow {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("time_ns", DataType::UInt64, false),
            Field::new("receiver_idx", DataType::UInt32, false),
            Field::new("peer_id", DataType::UInt64, false),
            Field::new("kind", DataType::UInt8, false),
            Field::new("amount", DataType::UInt32, false),
            Field::new("total_diff", DataType::UInt32, false),
        ]))
    }

    fn to_batch(rows: &[Self]) -> RecordBatch {
        let n = rows.len();
        let mut time_ns = UInt64Builder::with_capacity(n);
        let mut receiver_idx = UInt32Builder::with_capacity(n);
        let mut peer_id = UInt64Builder::with_capacity(n);
        let mut kind = UInt8Builder::with_capacity(n);
        let mut amount = UInt32Builder::with_capacity(n);
        let mut total_diff = UInt32Builder::with_capacity(n);
        for r in rows {
            time_ns.append_value(r.time_ns);
            receiver_idx.append_value(r.receiver_idx);
            peer_id.append_value(r.peer_id);
            kind.append_value(r.kind);
            amount.append_value(r.amount);
            total_diff.append_value(r.total_diff);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(time_ns.finish()),
            Arc::new(receiver_idx.finish()),
            Arc::new(peer_id.finish()),
            Arc::new(kind.finish()),
            Arc::new(amount.finish()),
            Arc::new(total_diff.finish()),
        ];
        RecordBatch::try_new(Self::schema(), columns).expect("OverflowEventRow batch")
    }
}

impl WriteRow for RunMetaRow {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("seed", DataType::UInt64, false),
            Field::new("n_nodes", DataType::UInt64, false),
            Field::new("mean_degree", DataType::Float64, false),
            Field::new("min_degree", DataType::UInt64, false),
            Field::new("max_degree", DataType::UInt64, false),
            Field::new("diameter", DataType::UInt64, false),
            Field::new("mean_path_length", DataType::Float64, false),
            Field::new("stagger_secs", DataType::Float64, false),
            Field::new("capacity_chan_updates", DataType::UInt64, false),
            Field::new("capacity_node_anns", DataType::UInt64, false),
            Field::new("capacity_chan_anns", DataType::UInt64, false),
            Field::new("events_chan_update", DataType::UInt64, false),
            Field::new("events_node_ann", DataType::UInt64, false),
            Field::new("events_chan_ann", DataType::UInt64, false),
            Field::new("duration_seconds", DataType::UInt64, false),
            Field::new("predicted_p50_secs", DataType::Float64, false),
            Field::new("predicted_p99_secs", DataType::Float64, false),
            Field::new("predicted_p100_secs", DataType::Float64, false),
            Field::new("algo", DataType::Utf8, false),
            Field::new("topology_kind", DataType::Utf8, false),
            Field::new("event_kind", DataType::Utf8, false),
        ]))
    }

    fn to_batch(rows: &[Self]) -> RecordBatch {
        let n = rows.len();
        let mut seed = UInt64Builder::with_capacity(n);
        let mut n_nodes = UInt64Builder::with_capacity(n);
        let mut mean_degree = Float64Builder::with_capacity(n);
        let mut min_degree = UInt64Builder::with_capacity(n);
        let mut max_degree = UInt64Builder::with_capacity(n);
        let mut diameter = UInt64Builder::with_capacity(n);
        let mut mean_path_length = Float64Builder::with_capacity(n);
        let mut stagger_secs = Float64Builder::with_capacity(n);
        let mut cap_cu = UInt64Builder::with_capacity(n);
        let mut cap_na = UInt64Builder::with_capacity(n);
        let mut cap_ca = UInt64Builder::with_capacity(n);
        let mut ev_cu = UInt64Builder::with_capacity(n);
        let mut ev_na = UInt64Builder::with_capacity(n);
        let mut ev_ca = UInt64Builder::with_capacity(n);
        let mut duration = UInt64Builder::with_capacity(n);
        let mut p50 = Float64Builder::with_capacity(n);
        let mut p99 = Float64Builder::with_capacity(n);
        let mut p100 = Float64Builder::with_capacity(n);
        let mut algo = StringBuilder::with_capacity(n, n * 16);
        let mut topology_kind = StringBuilder::with_capacity(n, n * 16);
        let mut event_kind = StringBuilder::with_capacity(n, n * 16);
        for r in rows {
            seed.append_value(r.seed);
            n_nodes.append_value(r.n_nodes);
            mean_degree.append_value(r.mean_degree);
            min_degree.append_value(r.min_degree);
            max_degree.append_value(r.max_degree);
            diameter.append_value(r.diameter);
            mean_path_length.append_value(r.mean_path_length);
            stagger_secs.append_value(r.stagger_secs);
            cap_cu.append_value(r.capacity_chan_updates);
            cap_na.append_value(r.capacity_node_anns);
            cap_ca.append_value(r.capacity_chan_anns);
            ev_cu.append_value(r.events_chan_update);
            ev_na.append_value(r.events_node_ann);
            ev_ca.append_value(r.events_chan_ann);
            duration.append_value(r.duration_seconds);
            p50.append_value(r.predicted_p50_secs);
            p99.append_value(r.predicted_p99_secs);
            p100.append_value(r.predicted_p100_secs);
            algo.append_value(&r.algo);
            topology_kind.append_value(&r.topology_kind);
            event_kind.append_value(&r.event_kind);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(seed.finish()),
            Arc::new(n_nodes.finish()),
            Arc::new(mean_degree.finish()),
            Arc::new(min_degree.finish()),
            Arc::new(max_degree.finish()),
            Arc::new(diameter.finish()),
            Arc::new(mean_path_length.finish()),
            Arc::new(stagger_secs.finish()),
            Arc::new(cap_cu.finish()),
            Arc::new(cap_na.finish()),
            Arc::new(cap_ca.finish()),
            Arc::new(ev_cu.finish()),
            Arc::new(ev_na.finish()),
            Arc::new(ev_ca.finish()),
            Arc::new(duration.finish()),
            Arc::new(p50.finish()),
            Arc::new(p99.finish()),
            Arc::new(p100.finish()),
            Arc::new(algo.finish()),
            Arc::new(topology_kind.finish()),
            Arc::new(event_kind.finish()),
        ];
        RecordBatch::try_new(Self::schema(), columns).expect("RunMetaRow batch")
    }
}

impl WriteRow for NodePubkeyRow {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("node_idx", DataType::UInt32, false),
            Field::new("node_id", DataType::UInt64, false),
            Field::new("pubkey", DataType::Utf8, false),
        ]))
    }

    fn to_batch(rows: &[Self]) -> RecordBatch {
        let n = rows.len();
        let mut node_idx = UInt32Builder::with_capacity(n);
        let mut node_id = UInt64Builder::with_capacity(n);
        let mut pubkey = StringBuilder::with_capacity(n, n * 66);
        for r in rows {
            node_idx.append_value(r.node_idx);
            node_id.append_value(r.node_id);
            pubkey.append_value(&r.pubkey);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(node_idx.finish()),
            Arc::new(node_id.finish()),
            Arc::new(pubkey.finish()),
        ];
        RecordBatch::try_new(Self::schema(), columns).expect("NodePubkeyRow batch")
    }
}

// `MsgStats` is special — its schema depends on the configured
// percentile list — so it doesn't fit the parameterless `WriteRow`
// trait. It gets its own pair of helpers below.

fn build_msg_schema(percentiles: &[f64]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("msg_id", DataType::UInt64, false),
        Field::new("coverage", DataType::UInt64, false),
        Field::new("n_nodes", DataType::UInt64, false),
        Field::new("origin_ns", DataType::UInt64, false),
        Field::new("last_ns", DataType::UInt64, false),
        Field::new("size_bytes", DataType::UInt32, false),
    ];
    for &p in percentiles {
        fields.push(Field::new(pct_col(p), DataType::UInt64, true));
    }
    Arc::new(Schema::new(fields))
}

fn msg_to_batch(schema: &Arc<Schema>, percentiles: &[f64], rows: &[MsgStats]) -> RecordBatch {
    let n = rows.len();
    let mut msg_id = UInt64Builder::with_capacity(n);
    let mut coverage = UInt64Builder::with_capacity(n);
    let mut n_nodes = UInt64Builder::with_capacity(n);
    let mut origin_ns = UInt64Builder::with_capacity(n);
    let mut last_ns = UInt64Builder::with_capacity(n);
    let mut size_bytes = UInt32Builder::with_capacity(n);
    let mut pct_builders: Vec<UInt64Builder> = (0..percentiles.len())
        .map(|_| UInt64Builder::with_capacity(n))
        .collect();
    for r in rows {
        msg_id.append_value(r.id);
        coverage.append_value(r.coverage as u64);
        n_nodes.append_value(r.n_nodes as u64);
        origin_ns.append_value(r.origin_ns);
        last_ns.append_value(r.last_ns);
        size_bytes.append_value(r.size_bytes);
        for (i, _p) in percentiles.iter().enumerate() {
            let v = r.percentiles.get(i).and_then(|(_, d)| *d).map(|d| d.as_nanos() as u64);
            pct_builders[i].append_option(v);
        }
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(msg_id.finish()),
        Arc::new(coverage.finish()),
        Arc::new(n_nodes.finish()),
        Arc::new(origin_ns.finish()),
        Arc::new(last_ns.finish()),
        Arc::new(size_bytes.finish()),
    ];
    for mut b in pct_builders {
        columns.push(Arc::new(b.finish()));
    }
    RecordBatch::try_new(schema.clone(), columns).expect("MsgStats batch")
}

// ---- output paths + writer thread ----------------------------------

/// All output filenames for a given tag prefix.
pub struct OutputPaths {
    pub tag: PathBuf,
    pub msg_stats: PathBuf,
    pub node_counters: PathBuf,
    pub node_reservoirs: PathBuf,
    pub overflow_events: PathBuf,
    pub run_meta: PathBuf,
    pub node_pubkey: PathBuf,
}

impl OutputPaths {
    pub fn from_tag(tag: &Path) -> Self {
        let s = tag.to_string_lossy();
        let mk = |suffix: &str| PathBuf::from(format!("{s}-{suffix}.parquet"));
        Self {
            tag: tag.to_path_buf(),
            msg_stats: mk("msg_stats"),
            node_counters: mk("node_counters"),
            node_reservoirs: mk("node_reservoirs"),
            overflow_events: mk("overflow_events"),
            run_meta: mk("run_meta"),
            node_pubkey: mk("node_pubkey"),
        }
    }
}

pub struct Writer {
    tx: RowSender,
    joins: Vec<JoinHandle<()>>,
}

impl Writer {
    pub fn sender(&self) -> RowSender {
        self.tx.clone()
    }

    /// Drop all senders, then join every per-file writer thread.
    pub fn close(self) {
        let Self { tx, joins } = self;
        drop(tx);
        for h in joins {
            h.join().expect("stats writer thread panicked");
        }
    }
}

/// Spawn one writer thread per output Parquet file. Hot files
/// (`msg_stats`, `node_counters`, `overflow_events`) use ZSTD-1 for
/// throughput; the other three keep the default level.
pub fn spawn(paths: OutputPaths, percentiles: Vec<f64>) -> Writer {
    let zstd_fast = Compression::ZSTD(ZstdLevel::try_new(1).expect("ZstdLevel(1)"));
    let zstd_default = Compression::ZSTD(ZstdLevel::default());

    let (msg_tx, msg_rx) = bounded::<MsgStats>(PER_STREAM_DEPTH);
    let msg_path = paths.msg_stats.clone();
    let msg_join = thread::spawn(move || run_msg_stats(msg_path, percentiles, msg_rx));

    let (counters_tx, counters_rx) = bounded::<NodeCountersRow>(PER_STREAM_DEPTH);
    let counters_path = paths.node_counters.clone();
    let counters_join = thread::spawn(move || {
        run_stream::<NodeCountersRow>(&counters_path, zstd_fast, counters_rx, "node_counters")
    });

    let (reservoirs_tx, reservoirs_rx) = bounded::<NodeReservoirRow>(PER_STREAM_DEPTH);
    let reservoirs_path = paths.node_reservoirs.clone();
    let reservoirs_join = thread::spawn(move || {
        run_stream::<NodeReservoirRow>(
            &reservoirs_path,
            zstd_default,
            reservoirs_rx,
            "node_reservoirs",
        )
    });

    let (overflow_tx, overflow_rx) = bounded::<OverflowEventRow>(PER_STREAM_DEPTH);
    let overflow_path = paths.overflow_events.clone();
    let overflow_join = thread::spawn(move || {
        run_stream::<OverflowEventRow>(&overflow_path, zstd_fast, overflow_rx, "overflow_events")
    });

    let (run_meta_tx, run_meta_rx) = bounded::<RunMetaRow>(PER_STREAM_DEPTH);
    let run_meta_path = paths.run_meta.clone();
    let run_meta_join = thread::spawn(move || {
        run_stream::<RunMetaRow>(&run_meta_path, zstd_default, run_meta_rx, "run_meta")
    });

    let (pubkey_tx, pubkey_rx) = bounded::<NodePubkeyRow>(PER_STREAM_DEPTH);
    let pubkey_path = paths.node_pubkey.clone();
    let pubkey_join = thread::spawn(move || {
        run_stream::<NodePubkeyRow>(&pubkey_path, zstd_default, pubkey_rx, "node_pubkey")
    });

    Writer {
        tx: RowSender {
            msg_stats: msg_tx,
            node_counters: counters_tx,
            node_reservoirs: reservoirs_tx,
            overflow_events: overflow_tx,
            run_meta: run_meta_tx,
            node_pubkey: pubkey_tx,
        },
        joins: vec![
            msg_join,
            counters_join,
            reservoirs_join,
            overflow_join,
            run_meta_join,
            pubkey_join,
        ],
    }
}

/// Per-stream state: an open writer + a typed in-memory buffer.
/// `flush` encodes the buffer to Parquet via `R::to_batch`.
struct Stream<R: WriteRow> {
    writer: ArrowWriter<File>,
    buf: Vec<R>,
}

impl<R: WriteRow> Stream<R> {
    fn open_with_compression(path: &Path, compression: Compression) -> Self {
        let file = File::create(path)
            .unwrap_or_else(|e| panic!("create parquet file {}: {e}", path.display()));
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let writer = ArrowWriter::try_new(file, R::schema(), Some(props))
            .expect("init parquet ArrowWriter");
        Self {
            writer,
            buf: Vec::with_capacity(FLUSH_EVERY),
        }
    }

    fn push(&mut self, row: R) {
        self.buf.push(row);
        if self.buf.len() >= FLUSH_EVERY {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let rb = R::to_batch(&self.buf);
        self.writer.write(&rb).expect("write parquet batch");
        self.buf.clear();
    }

    fn close(mut self, label: &'static str) {
        self.flush();
        self.writer
            .close()
            .unwrap_or_else(|e| panic!("close parquet writer {label}: {e}"));
    }
}

/// Generic per-file worker: drains its channel into one `Stream<R>`,
/// closing on disconnect. One per non-msg_stats Parquet file.
fn run_stream<R: WriteRow + Send + 'static>(
    path: &Path,
    compression: Compression,
    rx: Receiver<R>,
    label: &'static str,
) {
    let mut stream = Stream::<R>::open_with_compression(path, compression);
    for row in rx {
        stream.push(row);
    }
    stream.close(label);
}

/// Bespoke writer for `msg_stats` — its schema depends on the
/// runtime-configured percentile list, so it doesn't fit the
/// parameterless `WriteRow` trait. Same buffering shape as
/// `Stream<R>`, just inlined here.
fn run_msg_stats(path: PathBuf, percentiles: Vec<f64>, rx: Receiver<MsgStats>) {
    let schema = build_msg_schema(&percentiles);
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).expect("ZstdLevel(1)")))
        .build();
    let file = File::create(&path)
        .unwrap_or_else(|e| panic!("create msg_stats parquet {}: {e}", path.display()));
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("init msg_stats ArrowWriter");
    let mut buf: Vec<MsgStats> = Vec::with_capacity(FLUSH_EVERY);

    let flush = |w: &mut ArrowWriter<File>, buf: &mut Vec<MsgStats>| {
        if buf.is_empty() {
            return;
        }
        let rb = msg_to_batch(&schema, &percentiles, buf);
        w.write(&rb).expect("write msg_stats batch");
        buf.clear();
    };

    for row in rx {
        buf.push(row);
        if buf.len() >= FLUSH_EVERY {
            flush(&mut writer, &mut buf);
        }
    }
    flush(&mut writer, &mut buf);
    writer.close().expect("close msg_stats parquet");
}

// ---- helpers --------------------------------------------------------

fn pct_col(p: f64) -> String {
    let v = p * 100.0;
    let r = v.round();
    if (v - r).abs() < 1e-9 {
        format!("p{}_ns", r as u32)
    } else {
        let s = format!("{:.1}", v).replace('.', "_");
        format!("p{}_ns", s)
    }
}

/// `{topology}-{algo}-{event}-{YYYY-MM-DD-HHMM}` — shared filename
/// stem all six Parquet files derive from.
pub fn auto_tag(topology: &str, algo: &str, event: &str) -> PathBuf {
    let stamp = chrono::Local::now().format("%Y-%m-%d-%H%M").to_string();
    PathBuf::from(format!("{topology}-{algo}-{event}-{stamp}"))
}

/// Convenience used by tests to assert all 6 output files were produced.
#[allow(dead_code)]
pub(crate) fn file_exists(p: &Path) -> bool {
    p.exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pct_col_integer_fractions() {
        assert_eq!(pct_col(0.25), "p25_ns");
        assert_eq!(pct_col(0.5), "p50_ns");
        assert_eq!(pct_col(0.75), "p75_ns");
        assert_eq!(pct_col(1.0), "p100_ns");
    }

    #[test]
    fn pct_col_fractional() {
        assert_eq!(pct_col(0.333), "p33_3_ns");
        assert_eq!(pct_col(0.9999), "p100_0_ns");
    }

    #[test]
    fn output_paths_share_tag() {
        let paths = OutputPaths::from_tag(Path::new("foo-bar-baz-2026-05-12"));
        assert_eq!(
            paths.msg_stats,
            PathBuf::from("foo-bar-baz-2026-05-12-msg_stats.parquet")
        );
        assert_eq!(
            paths.node_counters,
            PathBuf::from("foo-bar-baz-2026-05-12-node_counters.parquet")
        );
        assert_eq!(
            paths.overflow_events,
            PathBuf::from("foo-bar-baz-2026-05-12-overflow_events.parquet")
        );
    }
}
