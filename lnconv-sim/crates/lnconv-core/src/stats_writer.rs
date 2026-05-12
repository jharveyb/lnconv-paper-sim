//! Background Parquet writer for finalised `MsgStats`.
//!
//! At sim init the runner spawns one writer thread per `MetricsHandle`.
//! The thread owns an `ArrowWriter<File>` plus an in-memory mirror of
//! every `MsgStats` it received, then exits when the channel closes.
//! `MetricsHandle::close_writer` drops the sender to signal EOF, joins
//! the thread, and recovers the mirror Vec — that's what
//! `completed_stats()` returns to the CLI.
//!
//! Schema (one row per finalised message):
//!
//! ```text
//! msg_id    : UInt64
//! coverage  : UInt64
//! n_nodes   : UInt64
//! origin_ns : UInt64
//! last_ns   : UInt64
//! p25_ns    : UInt64?   (one nullable column per configured percentile;
//!                       column name derived from the f64 fraction)
//! ...
//! ```
//!
//! `Option<Duration>::None` (CLAUDE.md invariant #6: partial coverage
//! can't reach the percentile) writes as a NULL.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::metrics::MsgStats;

/// Rows are accumulated in memory in batches of this size, then
/// flushed to the parquet writer as one `RecordBatch`. Keeps peak
/// per-row dispatch overhead low while bounding memory.
const FLUSH_EVERY: usize = 1024;

/// Spawn the writer thread. Returns the sender end of the mpsc channel
/// (cheap to clone via the surrounding `Arc<Metrics>` but typically
/// held as a single shared sender) plus the join handle. On shutdown
/// drop the sender and `.join()` the handle to recover the mirror.
pub fn spawn(
    path: PathBuf,
    percentiles: Vec<f64>,
) -> (Sender<MsgStats>, JoinHandle<Vec<MsgStats>>) {
    let (tx, rx) = mpsc::channel::<MsgStats>();
    let handle = thread::spawn(move || run(path, percentiles, rx));
    (tx, handle)
}

fn run(path: PathBuf, percentiles: Vec<f64>, rx: Receiver<MsgStats>) -> Vec<MsgStats> {
    let schema = build_schema(&percentiles);
    let file = File::create(&path).expect("create stats parquet");
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("init parquet ArrowWriter");

    let mut buf: Vec<MsgStats> = Vec::with_capacity(FLUSH_EVERY);
    let mut mirror: Vec<MsgStats> = Vec::new();
    for stats in rx {
        mirror.push(stats.clone());
        buf.push(stats);
        if buf.len() >= FLUSH_EVERY {
            let rb = to_record_batch(&schema, &percentiles, &buf);
            writer.write(&rb).expect("write parquet batch");
            buf.clear();
        }
    }
    if !buf.is_empty() {
        let rb = to_record_batch(&schema, &percentiles, &buf);
        writer.write(&rb).expect("write parquet batch (final)");
    }
    writer.close().expect("close parquet writer");
    mirror
}

fn build_schema(percentiles: &[f64]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("msg_id", DataType::UInt64, false),
        Field::new("coverage", DataType::UInt64, false),
        Field::new("n_nodes", DataType::UInt64, false),
        Field::new("origin_ns", DataType::UInt64, false),
        Field::new("last_ns", DataType::UInt64, false),
    ];
    for &p in percentiles {
        fields.push(Field::new(pct_col(p), DataType::UInt64, true));
    }
    Arc::new(Schema::new(fields))
}

fn to_record_batch(schema: &Arc<Schema>, percentiles: &[f64], rows: &[MsgStats]) -> RecordBatch {
    let n = rows.len();
    let mut msg_id = Vec::with_capacity(n);
    let mut coverage = Vec::with_capacity(n);
    let mut n_nodes = Vec::with_capacity(n);
    let mut origin_ns = Vec::with_capacity(n);
    let mut last_ns = Vec::with_capacity(n);
    let mut pct_cols: Vec<Vec<Option<u64>>> = (0..percentiles.len())
        .map(|_| Vec::with_capacity(n))
        .collect();
    for r in rows {
        msg_id.push(r.id);
        coverage.push(r.coverage as u64);
        n_nodes.push(r.n_nodes as u64);
        origin_ns.push(r.origin_ns);
        last_ns.push(r.last_ns);
        // `MsgStats.percentiles` is `Vec<(f64, Option<Duration>)>`
        // in the same order as the configured list — index by position.
        for (i, _p) in percentiles.iter().enumerate() {
            let v = r.percentiles.get(i).and_then(|(_, d)| *d).map(|d| d.as_nanos() as u64);
            pct_cols[i].push(v);
        }
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(msg_id)),
        Arc::new(UInt64Array::from(coverage)),
        Arc::new(UInt64Array::from(n_nodes)),
        Arc::new(UInt64Array::from(origin_ns)),
        Arc::new(UInt64Array::from(last_ns)),
    ];
    for col in pct_cols {
        columns.push(Arc::new(UInt64Array::from(col)));
    }
    RecordBatch::try_new(schema.clone(), columns).expect("build RecordBatch")
}

/// Column name for a percentile expressed as a 0..=1 fraction.
///   `0.25` → `"p25_ns"`, `0.5` → `"p50_ns"`, `1.0` → `"p100_ns"`,
///   `0.333` → `"p33_3_ns"` (one-decimal precision; `.` replaced by `_`).
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

/// Sanity helper used by the runner to compute a stable, human-readable
/// output filename: `{topology}-{algo}-{event}-{YYYY-MM-DD-HHMM}.parquet`.
pub fn auto_path(topology: &str, algo: &str, event: &str) -> PathBuf {
    let stamp = chrono::Local::now().format("%Y-%m-%d-%H%M").to_string();
    PathBuf::from(format!("{topology}-{algo}-{event}-{stamp}.parquet"))
}

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
}
