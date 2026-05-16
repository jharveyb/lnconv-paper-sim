//! DuckDB-based end-of-run summary report.
//!
//! Every CLI summary table is computed via DuckDB SQL over the six
//! Parquet files emitted by `stats_writer`. Lives in lnconv-core so
//! the CLI binary stays a thin skeleton.
//!
//! ## Files registered as views
//!
//! | view name         | source file                  |
//! |-------------------|------------------------------|
//! | `msgs`            | `T-msg_stats.parquet`        |
//! | `counters`        | `T-node_counters.parquet`    |
//! | `reservoirs`      | `T-node_reservoirs.parquet`  |
//! | `overflow_events` | `T-overflow_events.parquet`  |
//! | `run_meta`        | `T-run_meta.parquet`         |
//! | `node_pubkey`     | `T-node_pubkey.parquet`      |
//!
//! Each `print_*` function defines a small POD struct that mirrors its
//! query's SELECT list and implements `TryFrom<&Row>` for the column
//! reads — replaces the 21-line `let foo: T = row.get(N)?` ladders we
//! used to have.

use std::path::Path;

use anyhow::{Context, Result};
use duckdb::{Connection, Row};

use crate::message::SketchKind;
use crate::stats_writer::OutputPaths;

/// Entry point: run all canned queries against the six Parquet files
/// derived from `tag_prefix`, printing each block to stdout.
pub fn run_summary_report(tag_prefix: &Path) -> Result<()> {
    let paths = OutputPaths::from_tag(tag_prefix);
    let conn = Connection::open_in_memory().context("open DuckDB in-memory connection")?;
    register_views(&conn, &paths)?;

    print_run_meta(&conn)?;
    print_per_node_bandwidth(&conn)?;
    print_sketch_totals(&conn)?;
    print_sketch_reconciliation_per_kind(&conn)?;
    print_sketch_rounds_distribution(&conn)?;
    print_overflow_summary(&conn)?;
    print_top_overflow_pairs(&conn, 10)?;
    print_per_message_table(&conn, 5)?;
    print_coverage_distribution(&conn)?;
    print_per_tier_distribution(&conn)?;
    Ok(())
}

fn register_views(conn: &Connection, p: &OutputPaths) -> Result<()> {
    for (view, path) in [
        ("msgs", p.msg_stats.as_path()),
        ("counters", p.node_counters.as_path()),
        ("reservoirs", p.node_reservoirs.as_path()),
        ("overflow_events", p.overflow_events.as_path()),
        ("run_meta", p.run_meta.as_path()),
        ("node_pubkey", p.node_pubkey.as_path()),
    ] {
        let sql = format!(
            "CREATE VIEW {view} AS SELECT * FROM read_parquet('{}')",
            path.display()
        );
        conn.execute_batch(&sql)
            .with_context(|| format!("register view `{view}` from {}", path.display()))?;
    }
    Ok(())
}

// ---- shared helpers -------------------------------------------------

/// `quantile_cont` and friends return f64 even when the underlying
/// column is u64. Casting once here keeps the call sites tidy.
fn u64_from_f64(row: &Row, i: usize) -> u64 {
    row.get::<usize, f64>(i).map(|v| v as u64).unwrap_or(0)
}

/// Render `ns` as a human-readable duration with the same widths the
/// previous Rust CLI used (so output diffs stay narrow).
fn fmt_dur_ns(ns: u64) -> String {
    let ms = ns as f64 / 1e6;
    if ms < 1.0 {
        format!("{:>7.0}μs", ns as f64 / 1e3)
    } else if ms < 1000.0 {
        format!("{ms:>8.1}ms")
    } else {
        format!("{:>8.2}s ", ms / 1000.0)
    }
}

/// Render one `(label, min, p50, mean, p95, max, total)` row using the
/// canonical column widths shared across every bandwidth-style table.
fn print_minmaxtotal(label: &str, min: u64, p50: u64, mean: u64, p95: u64, max: u64, total: u64) {
    println!(
        "  {label:<22}min={min:>10}  p50={p50:>10}  mean={mean:>10}  p95={p95:>10}  max={max:>10}  total={total}"
    );
}

// ---- run_meta -------------------------------------------------------

struct RunMetaSnapshot {
    seed: u64,
    n_nodes: u64,
    mean_degree: f64,
    min_degree: u64,
    max_degree: u64,
    diameter: u64,
    mean_path_length: f64,
    stagger_secs: f64,
    cap_cu: u64,
    cap_na: u64,
    cap_ca: u64,
    ev_cu: u64,
    ev_na: u64,
    ev_ca: u64,
    duration_seconds: u64,
    wall_time_secs: f64,
    p50_secs: f64,
    p99_secs: f64,
    p100_secs: f64,
    algo: String,
    topology_kind: String,
    event_kind: String,
}

impl<'a> TryFrom<&Row<'a>> for RunMetaSnapshot {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            seed: row.get(0)?,
            n_nodes: row.get(1)?,
            mean_degree: row.get(2)?,
            min_degree: row.get(3)?,
            max_degree: row.get(4)?,
            diameter: row.get(5)?,
            mean_path_length: row.get(6)?,
            stagger_secs: row.get(7)?,
            cap_cu: row.get(8)?,
            cap_na: row.get(9)?,
            cap_ca: row.get(10)?,
            ev_cu: row.get(11)?,
            ev_na: row.get(12)?,
            ev_ca: row.get(13)?,
            duration_seconds: row.get(14)?,
            wall_time_secs: row.get(15)?,
            p50_secs: row.get(16)?,
            p99_secs: row.get(17)?,
            p100_secs: row.get(18)?,
            algo: row.get(19)?,
            topology_kind: row.get(20)?,
            event_kind: row.get(21)?,
        })
    }
}

fn print_run_meta(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT seed, n_nodes, mean_degree, min_degree, max_degree, diameter,
                mean_path_length, stagger_secs,
                capacity_chan_updates, capacity_node_anns, capacity_chan_anns,
                events_chan_update, events_node_ann, events_chan_ann,
                duration_seconds, wall_time_secs,
                predicted_p50_secs, predicted_p99_secs, predicted_p100_secs,
                algo, topology_kind, event_kind
         FROM run_meta",
    )?;
    let m: RunMetaSnapshot = match stmt.query_and_then([], |r| RunMetaSnapshot::try_from(r))?.next() {
        Some(r) => r?,
        None => return Ok(()),
    };
    let total_events = m.ev_cu + m.ev_na + m.ev_ca;
    let secs = m.duration_seconds.max(1) as f64;
    println!();
    println!("run summary (from run_meta-*.parquet):");
    println!(
        "  seed={}  algo={}  topology={}  event={}",
        m.seed, m.algo, m.topology_kind, m.event_kind
    );
    let wall_pretty = if m.wall_time_secs > 0.0 {
        format!("  wall_time={:.2}s", m.wall_time_secs)
    } else {
        String::new()
    };
    println!(
        "  duration={}s{wall_pretty}  n_nodes={}  mean_degree={:.2} min={} max={}  diameter={}  L̄={:.2}",
        m.duration_seconds,
        m.n_nodes,
        m.mean_degree,
        m.min_degree,
        m.max_degree,
        m.diameter,
        m.mean_path_length
    );
    if m.stagger_secs > 0.0 {
        println!(
            "  sketch: σ={}s  capacities chan_updates={} node_anns={} chan_anns={}",
            m.stagger_secs, m.cap_cu, m.cap_na, m.cap_ca
        );
        println!(
            "  predicted coverage: p50={:.1}s  p99={:.1}s  p100={:.1}s",
            m.p50_secs, m.p99_secs, m.p100_secs
        );
    }
    if total_events > 0 {
        let pct = |x: u64| -> f64 { 100.0 * x as f64 / total_events as f64 };
        let rate = |x: u64| -> f64 { x as f64 / secs };
        println!(
            "  events: chan_update={} ({:.1}%, {:.2}/s)  node_ann={} ({:.1}%, {:.2}/s)  chan_ann={} ({:.1}%, {:.2}/s)",
            m.ev_cu, pct(m.ev_cu), rate(m.ev_cu),
            m.ev_na, pct(m.ev_na), rate(m.ev_na),
            m.ev_ca, pct(m.ev_ca), rate(m.ev_ca),
        );
    }
    print_unique_data_block(conn, m.n_nodes, secs)?;
    Ok(())
}

/// Returns a human-readable byte count using SI prefixes (kB, MB, GB).
/// Matches the level of precision shown in the rest of `run summary`.
fn fmt_bytes(b: f64) -> String {
    const K: f64 = 1_000.0;
    if b < K {
        format!("{b:.0} B")
    } else if b < K * K {
        format!("{:.2} kB", b / K)
    } else if b < K * K * K {
        format!("{:.2} MB", b / (K * K))
    } else {
        format!("{:.2} GB", b / (K * K * K))
    }
}

fn print_unique_data_block(conn: &Connection, n_nodes: u64, secs: f64) -> Result<()> {
    let sql = "
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY node_idx ORDER BY time_ns DESC) AS rn
            FROM counters
        ),
        finals AS (SELECT * FROM latest WHERE rn = 1),
        msg_agg AS (SELECT SUM(size_bytes) AS unique_bytes, COUNT(*) AS msg_count FROM msgs),
        in_agg AS (
            SELECT SUM(bytes_in_gossip) AS in_gossip,
                   SUM(bytes_in_sketch) AS in_sketch,
                   SUM(bytes_in_inventory) AS in_invent
            FROM finals
        )
        SELECT unique_bytes, msg_count, in_gossip, in_sketch, in_invent
        FROM msg_agg, in_agg";
    let mut stmt = conn.prepare(sql)?;
    let (unique_bytes, msg_count, in_gossip, in_sketch, in_invent): (f64, u64, f64, f64, f64) =
        match stmt
            .query_and_then([], |r| -> Result<_, duckdb::Error> {
                Ok((
                    r.get::<usize, f64>(0).unwrap_or(0.0),
                    r.get::<usize, u64>(1).unwrap_or(0),
                    r.get::<usize, f64>(2).unwrap_or(0.0),
                    r.get::<usize, f64>(3).unwrap_or(0.0),
                    r.get::<usize, f64>(4).unwrap_or(0.0),
                ))
            })?
            .next()
        {
            Some(r) => r?,
            None => return Ok(()),
        };
    if msg_count == 0 || unique_bytes <= 0.0 {
        return Ok(());
    }
    let avg_size = unique_bytes / msg_count as f64;
    let rate = unique_bytes / secs;
    // Protocol-overhead factor: how much total inbound traffic each
    // node receives relative to the gossip-only inbound. Values
    // > 1.0 mean sketch + inventory overhead dominates the
    // gossip payload; 1.0 means no reconciliation overhead. Computed
    // over network sums (in_gossip is the per-link cost summed across
    // all receivers, same shape as in_sketch and in_invent).
    let overhead = if in_gossip > 0.0 {
        (in_gossip + in_sketch + in_invent) / in_gossip
    } else {
        0.0
    };
    let _ = n_nodes;
    println!("  avg msg size: {avg_size:.1} bytes  ({} msgs)", msg_count);
    println!(
        "  total unique data: {}  ({}/s over {:.0}s)",
        fmt_bytes(unique_bytes),
        fmt_bytes(rate),
        secs
    );
    println!(
        "  bandwidth overhead factor: {overhead:.2}  (total received bytes / gossip received bytes)"
    );
    Ok(())
}

// ---- per-node bandwidth --------------------------------------------

/// One row of (min, p50, p95, max, total) — matches the bandwidth
/// table's per-column shape.
#[derive(Debug, Default)]
struct MmStats {
    min: u64,
    p50: u64,
    p95: u64,
    max: u64,
    total: u64,
}

impl MmStats {
    /// Read 5 consecutive columns starting at `start` as
    /// (min, p50, p95, max, total).
    fn read(row: &Row, start: usize) -> Self {
        Self {
            min: u64_from_f64(row, start),
            p50: u64_from_f64(row, start + 1),
            p95: u64_from_f64(row, start + 2),
            max: u64_from_f64(row, start + 3),
            total: u64_from_f64(row, start + 4),
        }
    }
}

struct PerNodeBandwidth {
    n: i64,
    bytes_in_gossip: MmStats,
    bytes_out_gossip: MmStats,
    bytes_in_sketch: MmStats,
    bytes_out_sketch: MmStats,
    bytes_in_inventory: MmStats,
    bytes_out_inventory: MmStats,
    duplicates: MmStats,
    duplicates_bytes: MmStats,
    any_sketch: bool,
    any_inventory: bool,
}

impl<'a> TryFrom<&Row<'a>> for PerNodeBandwidth {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        let n: i64 = row.get(0)?;
        Ok(Self {
            n,
            bytes_in_gossip: MmStats::read(row, 1),
            bytes_out_gossip: MmStats::read(row, 6),
            bytes_in_sketch: MmStats::read(row, 11),
            bytes_out_sketch: MmStats::read(row, 16),
            bytes_in_inventory: MmStats::read(row, 21),
            bytes_out_inventory: MmStats::read(row, 26),
            duplicates: MmStats::read(row, 31),
            duplicates_bytes: MmStats::read(row, 36),
            any_sketch: row.get::<usize, f64>(41).unwrap_or(0.0) > 0.0,
            any_inventory: row.get::<usize, f64>(42).unwrap_or(0.0) > 0.0,
        })
    }
}

fn print_per_node_bandwidth(conn: &Connection) -> Result<()> {
    let sql = "
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY node_idx ORDER BY time_ns DESC) AS rn
            FROM counters
        ),
        finals AS (SELECT * FROM latest WHERE rn = 1)
        SELECT
            COUNT(*),
            MIN(bytes_in_gossip),  quantile_cont(bytes_in_gossip, 0.50),
              quantile_cont(bytes_in_gossip, 0.95), MAX(bytes_in_gossip), SUM(bytes_in_gossip),
            MIN(bytes_out_gossip), quantile_cont(bytes_out_gossip, 0.50),
              quantile_cont(bytes_out_gossip, 0.95), MAX(bytes_out_gossip), SUM(bytes_out_gossip),
            MIN(bytes_in_sketch),  quantile_cont(bytes_in_sketch, 0.50),
              quantile_cont(bytes_in_sketch, 0.95), MAX(bytes_in_sketch), SUM(bytes_in_sketch),
            MIN(bytes_out_sketch), quantile_cont(bytes_out_sketch, 0.50),
              quantile_cont(bytes_out_sketch, 0.95), MAX(bytes_out_sketch), SUM(bytes_out_sketch),
            MIN(bytes_in_inventory),  quantile_cont(bytes_in_inventory, 0.50),
              quantile_cont(bytes_in_inventory, 0.95), MAX(bytes_in_inventory), SUM(bytes_in_inventory),
            MIN(bytes_out_inventory), quantile_cont(bytes_out_inventory, 0.50),
              quantile_cont(bytes_out_inventory, 0.95), MAX(bytes_out_inventory), SUM(bytes_out_inventory),
            MIN(duplicates),       quantile_cont(duplicates, 0.50),
              quantile_cont(duplicates, 0.95), MAX(duplicates), SUM(duplicates),
            MIN(duplicates_bytes), quantile_cont(duplicates_bytes, 0.50),
              quantile_cont(duplicates_bytes, 0.95), MAX(duplicates_bytes), SUM(duplicates_bytes),
            SUM(bytes_in_sketch) + SUM(bytes_out_sketch),
            SUM(bytes_in_inventory) + SUM(bytes_out_inventory)
        FROM finals";
    let mut stmt = conn.prepare(sql)?;
    let b: PerNodeBandwidth =
        match stmt.query_and_then([], |r| PerNodeBandwidth::try_from(r))?.next() {
            Some(r) => r?,
            None => return Ok(()),
        };
    println!("\nper-node bandwidth + duplicates (n={}):", b.n);
    let n = (b.n.max(1)) as u64;
    let mean = |total: u64| -> u64 { total / n };
    let g = &b.bytes_in_gossip;
    print_minmaxtotal("bytes_in  (gossip):", g.min, g.p50, mean(g.total), g.p95, g.max, g.total);
    let g = &b.bytes_out_gossip;
    print_minmaxtotal("bytes_out (gossip):", g.min, g.p50, mean(g.total), g.p95, g.max, g.total);
    if b.any_sketch {
        let s = &b.bytes_in_sketch;
        print_minmaxtotal("bytes_in  (sketch):", s.min, s.p50, mean(s.total), s.p95, s.max, s.total);
        let s = &b.bytes_out_sketch;
        print_minmaxtotal("bytes_out (sketch):", s.min, s.p50, mean(s.total), s.p95, s.max, s.total);
    }
    if b.any_inventory {
        let i = &b.bytes_in_inventory;
        print_minmaxtotal(
            "bytes_in  (invent):",
            i.min,
            i.p50,
            mean(i.total),
            i.p95,
            i.max,
            i.total,
        );
        let i = &b.bytes_out_inventory;
        print_minmaxtotal(
            "bytes_out (invent):",
            i.min,
            i.p50,
            mean(i.total),
            i.p95,
            i.max,
            i.total,
        );
    }
    let d = &b.duplicates;
    print_minmaxtotal("duplicates (msgs):", d.min, d.p50, mean(d.total), d.p95, d.max, d.total);
    let db = &b.duplicates_bytes;
    print_minmaxtotal("duplicates (bytes):", db.min, db.p50, mean(db.total), db.p95, db.max, db.total);
    Ok(())
}

// ---- sketch protocol totals ----------------------------------------

struct SketchTotals {
    sent: u64,
    received: u64,
    inv_sent: u64,
    inv_received: u64,
    o_cu: u64,
    o_na: u64,
    o_ca: u64,
    n_nodes: u64,
    ss_min: u64,
    ss_p50: u64,
    ss_p95: u64,
    ss_max: u64,
    sr_min: u64,
    sr_p50: u64,
    sr_p95: u64,
    sr_max: u64,
    is_min: u64,
    is_p50: u64,
    is_p95: u64,
    is_max: u64,
    ir_min: u64,
    ir_p50: u64,
    ir_p95: u64,
    ir_max: u64,
    /// True per-message min/max key count (the smallest / largest
    /// any node ever sent in one inventory). Mean is computed from
    /// `inv_keys_total / inv_sent`. p50/p95 come from the cross-node
    /// distribution of per-node averages — an approximation since we
    /// don't carry per-message samples.
    keys_per_msg_min: u64,
    keys_per_msg_max: u64,
    inv_keys_total: u64,
    keys_per_node_avg_p50: u64,
    keys_per_node_avg_p95: u64,
}

impl<'a> TryFrom<&Row<'a>> for SketchTotals {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            sent: u64_from_f64(row, 0),
            received: u64_from_f64(row, 1),
            inv_sent: u64_from_f64(row, 2),
            inv_received: u64_from_f64(row, 3),
            o_cu: u64_from_f64(row, 4),
            o_na: u64_from_f64(row, 5),
            o_ca: u64_from_f64(row, 6),
            n_nodes: u64_from_f64(row, 7),
            ss_min: u64_from_f64(row, 8),
            ss_p50: u64_from_f64(row, 9),
            ss_p95: u64_from_f64(row, 10),
            ss_max: u64_from_f64(row, 11),
            sr_min: u64_from_f64(row, 12),
            sr_p50: u64_from_f64(row, 13),
            sr_p95: u64_from_f64(row, 14),
            sr_max: u64_from_f64(row, 15),
            is_min: u64_from_f64(row, 16),
            is_p50: u64_from_f64(row, 17),
            is_p95: u64_from_f64(row, 18),
            is_max: u64_from_f64(row, 19),
            ir_min: u64_from_f64(row, 20),
            ir_p50: u64_from_f64(row, 21),
            ir_p95: u64_from_f64(row, 22),
            ir_max: u64_from_f64(row, 23),
            keys_per_msg_min: u64_from_f64(row, 24),
            keys_per_msg_max: u64_from_f64(row, 25),
            inv_keys_total: u64_from_f64(row, 26),
            keys_per_node_avg_p50: u64_from_f64(row, 27),
            keys_per_node_avg_p95: u64_from_f64(row, 28),
        })
    }
}

fn print_sketch_totals(conn: &Connection) -> Result<()> {
    let sql = "
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY node_idx ORDER BY time_ns DESC) AS rn
            FROM counters
        ),
        finals AS (SELECT * FROM latest WHERE rn = 1),
        per_node_keys AS (
            SELECT inv_keys_sent_sum / inventories_sent AS avg_keys
            FROM finals
            WHERE inventories_sent > 0
        )
        SELECT
            SUM(sketches_sent), SUM(sketches_received),
            SUM(inventories_sent), SUM(inventories_received),
            SUM(overflowed_chan_updates), SUM(overflowed_node_anns), SUM(overflowed_chan_anns),
            COUNT(*),
            MIN(sketches_sent), quantile_cont(sketches_sent, 0.5),
              quantile_cont(sketches_sent, 0.95), MAX(sketches_sent),
            MIN(sketches_received), quantile_cont(sketches_received, 0.5),
              quantile_cont(sketches_received, 0.95), MAX(sketches_received),
            MIN(inventories_sent), quantile_cont(inventories_sent, 0.5),
              quantile_cont(inventories_sent, 0.95), MAX(inventories_sent),
            MIN(inventories_received), quantile_cont(inventories_received, 0.5),
              quantile_cont(inventories_received, 0.95), MAX(inventories_received),
            MIN(inv_keys_sent_min) FILTER (WHERE inventories_sent > 0),
            MAX(inv_keys_sent_max),
            SUM(inv_keys_sent_sum),
            (SELECT quantile_cont(avg_keys, 0.5) FROM per_node_keys),
            (SELECT quantile_cont(avg_keys, 0.95) FROM per_node_keys)
        FROM finals";
    let mut stmt = conn.prepare(sql)?;
    let t: SketchTotals = match stmt.query_and_then([], |r| SketchTotals::try_from(r))?.next() {
        Some(r) => r?,
        None => return Ok(()),
    };
    if t.sent == 0 && t.received == 0 {
        return Ok(());
    }
    let total_overflows = t.o_cu + t.o_na + t.o_ca;
    let overflow_pct = if t.received > 0 {
        100.0 * total_overflows as f64 / t.received as f64
    } else {
        0.0
    };
    let n = t.n_nodes.max(1);
    let ss_mean = t.sent / n;
    let sr_mean = t.received / n;
    let is_mean = t.inv_sent / n;
    let ir_mean = t.inv_received / n;
    println!("\nsketch protocol:");
    println!("  sketches sent / received:    {} / {}", t.sent, t.received);
    if t.inv_sent > 0 || t.inv_received > 0 {
        println!(
            "  inventories sent / received: {} / {}",
            t.inv_sent, t.inv_received
        );
    }
    println!(
        "  overflows (diff > capacity): {total_overflows} ({overflow_pct:.1}%) — \
         chan_updates={}, node_anns={}, chan_anns={}",
        t.o_cu, t.o_na, t.o_ca
    );
    println!(
        "  per-node sketches_sent:        min={:>6} p50={:>6} mean={:>6} p95={:>6} max={:>6}",
        t.ss_min, t.ss_p50, ss_mean, t.ss_p95, t.ss_max
    );
    println!(
        "  per-node sketches_received:    min={:>6} p50={:>6} mean={:>6} p95={:>6} max={:>6}",
        t.sr_min, t.sr_p50, sr_mean, t.sr_p95, t.sr_max
    );
    if t.inv_sent > 0 || t.inv_received > 0 {
        println!(
            "  per-node inventories_sent:     min={:>6} p50={:>6} mean={:>6} p95={:>6} max={:>6}",
            t.is_min, t.is_p50, is_mean, t.is_p95, t.is_max
        );
        println!(
            "  per-node inventories_received: min={:>6} p50={:>6} mean={:>6} p95={:>6} max={:>6}",
            t.ir_min, t.ir_p50, ir_mean, t.ir_p95, t.ir_max
        );
        if t.inv_sent > 0 {
            // True per-message stats: min and max are the smallest/
            // largest single inventory message ever sent; mean is
            // total_keys / total_messages. p50/p95 are approximated
            // from cross-node averages (we don't carry per-message
            // samples, so an outlier-heavy node's tail is hidden).
            let mean_keys = t.inv_keys_total / t.inv_sent;
            println!(
                "  inventory keys per message:    min={:>6} p50={:>6} mean={:>6} p95={:>6} max={:>6} (p50/p95 over per-node averages)",
                t.keys_per_msg_min,
                t.keys_per_node_avg_p50,
                mean_keys,
                t.keys_per_node_avg_p95,
                t.keys_per_msg_max,
            );
        }
    }
    Ok(())
}

// ---- per-kind reconciliation totals --------------------------------

/// One (counter, min, p50, p95, max, total) row for one (kind,
/// counter) pair. Reused for the three (intersection / a_only / b_only)
/// queries per kind.
#[derive(Debug, Default)]
struct PerKindAgg {
    n: i64,
    intersection: MmStats,
    a_only: MmStats,
    b_only: MmStats,
    difference: MmStats,
}

impl<'a> TryFrom<&Row<'a>> for PerKindAgg {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            n: row.get(0)?,
            intersection: MmStats::read(row, 1),
            a_only: MmStats::read(row, 6),
            b_only: MmStats::read(row, 11),
            difference: MmStats::read(row, 16),
        })
    }
}

fn print_sketch_reconciliation_per_kind(conn: &Connection) -> Result<()> {
    let kinds = [SketchKind::ChanUpdates, SketchKind::NodeAnns, SketchKind::ChanAnns];
    let mut header_printed = false;
    for kind in kinds {
        let prefix = match kind {
            SketchKind::ChanUpdates => "chan_updates",
            SketchKind::NodeAnns => "node_anns",
            SketchKind::ChanAnns => "chan_anns",
        };
        let i_col = format!("{prefix}_intersection");
        let a_col = format!("{prefix}_a_only");
        let b_col = format!("{prefix}_b_only");
        let d_col = format!("{prefix}_difference");
        let sql = format!(
            "WITH latest AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY node_idx ORDER BY time_ns DESC) AS rn
                FROM counters
            ),
            finals AS (SELECT * FROM latest WHERE rn = 1)
            SELECT COUNT(*),
              MIN({i_col}), quantile_cont({i_col}, 0.5), quantile_cont({i_col}, 0.95),
                MAX({i_col}), SUM({i_col}),
              MIN({a_col}), quantile_cont({a_col}, 0.5), quantile_cont({a_col}, 0.95),
                MAX({a_col}), SUM({a_col}),
              MIN({b_col}), quantile_cont({b_col}, 0.5), quantile_cont({b_col}, 0.95),
                MAX({b_col}), SUM({b_col}),
              MIN({d_col}), quantile_cont({d_col}, 0.5), quantile_cont({d_col}, 0.95),
                MAX({d_col}), SUM({d_col})
            FROM finals"
        );
        let mut stmt = conn.prepare(&sql)?;
        let agg: PerKindAgg = match stmt.query_and_then([], |r| PerKindAgg::try_from(r))?.next() {
            Some(r) => r?,
            None => continue,
        };
        if !header_printed {
            println!(
                "\nsketch reconciliation per kind (per-node distribution; total = network sum):"
            );
            println!(
                "  {:<14} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>12} {:>16}",
                "kind", "counter", "min", "p50", "p95", "max", "mean", "total (net)"
            );
            header_printed = true;
        }
        let n = agg.n;
        let mean = |total: u64| -> f64 {
            if n > 0 { total as f64 / n as f64 } else { 0.0 }
        };
        for (label, st) in [
            ("intersection", &agg.intersection),
            ("difference", &agg.difference),
            ("a_only", &agg.a_only),
            ("b_only", &agg.b_only),
        ] {
            println!(
                "  {:<14} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>12.1} {:>16}",
                kind.as_label(), label, st.min, st.p50, st.p95, st.max, mean(st.total), st.total
            );
        }
    }
    Ok(())
}

// ---- per-round reservoir distribution ------------------------------

#[derive(Debug, Default)]
struct RoundsDistribution {
    kind: u8,
    n: i64,
    /// 4 sets of (min, p50, p95, p99, max, mean) for intersection,
    /// difference, a_only, b_only respectively.
    cols: [RoundCol; 4],
}

#[derive(Debug, Default)]
struct RoundCol {
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    mean: f64,
}

impl RoundCol {
    fn read(row: &Row, start: usize) -> Self {
        Self {
            min: u64_from_f64(row, start),
            p50: u64_from_f64(row, start + 1),
            p95: u64_from_f64(row, start + 2),
            p99: u64_from_f64(row, start + 3),
            max: u64_from_f64(row, start + 4),
            mean: row.get::<usize, f64>(start + 5).unwrap_or(0.0),
        }
    }
}

impl<'a> TryFrom<&Row<'a>> for RoundsDistribution {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: row.get(0)?,
            n: row.get(1)?,
            cols: [
                RoundCol::read(row, 2),
                RoundCol::read(row, 8),
                RoundCol::read(row, 14),
                RoundCol::read(row, 20),
            ],
        })
    }
}

fn print_sketch_rounds_distribution(conn: &Connection) -> Result<()> {
    let sql = "
        SELECT
            kind, COUNT(*),
            MIN(intersection), quantile_cont(intersection, 0.5),
              quantile_cont(intersection, 0.95), quantile_cont(intersection, 0.99),
              MAX(intersection), AVG(intersection),
            MIN(difference), quantile_cont(difference, 0.5),
              quantile_cont(difference, 0.95), quantile_cont(difference, 0.99),
              MAX(difference), AVG(difference),
            MIN(a_only), quantile_cont(a_only, 0.5),
              quantile_cont(a_only, 0.95), quantile_cont(a_only, 0.99),
              MAX(a_only), AVG(a_only),
            MIN(b_only), quantile_cont(b_only, 0.5),
              quantile_cont(b_only, 0.95), quantile_cont(b_only, 0.99),
              MAX(b_only), AVG(b_only)
        FROM reservoirs GROUP BY kind ORDER BY kind";
    let mut stmt = conn.prepare(sql)?;
    let rows: Vec<RoundsDistribution> = stmt
        .query_and_then([], |r| RoundsDistribution::try_from(r))?
        .collect::<Result<_, _>>()?;
    if rows.is_empty() {
        return Ok(());
    }
    println!("\nsketch reconciliation per kind (per-round distribution; reservoir-sampled):");
    println!(
        "  {:<14} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "kind", "counter", "n_rounds", "min", "p50", "p95", "p99", "max", "mean"
    );
    for r in rows {
        let kind = sketch_kind_from_u8(r.kind).as_label();
        for (label, col) in
            ["intersection", "difference", "a_only", "b_only"].iter().zip(&r.cols)
        {
            println!(
                "  {:<14} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10.1}",
                kind, label, r.n, col.min, col.p50, col.p95, col.p99, col.max, col.mean
            );
        }
    }
    Ok(())
}

// ---- overflow summary ----------------------------------------------

#[derive(Debug, Default)]
struct OverflowKind {
    kind: u8,
    event_count: i64,
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    mean: f64,
}

impl<'a> TryFrom<&Row<'a>> for OverflowKind {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: row.get(0)?,
            event_count: row.get(1)?,
            min: u64_from_f64(row, 2),
            p50: u64_from_f64(row, 3),
            p95: u64_from_f64(row, 4),
            p99: u64_from_f64(row, 5),
            max: u64_from_f64(row, 6),
            mean: row.get::<usize, f64>(7).unwrap_or(0.0),
        })
    }
}

fn print_overflow_summary(conn: &Connection) -> Result<()> {
    let sql = "
        SELECT
            kind, COUNT(*),
            MIN(amount), quantile_cont(amount, 0.5),
              quantile_cont(amount, 0.95), quantile_cont(amount, 0.99),
              MAX(amount), AVG(amount)
        FROM overflow_events GROUP BY kind ORDER BY kind";
    let mut stmt = conn.prepare(sql)?;
    let rows: Vec<OverflowKind> = stmt
        .query_and_then([], |r| OverflowKind::try_from(r))?
        .collect::<Result<_, _>>()?;
    if rows.is_empty() {
        return Ok(());
    }
    println!(
        "\nsketch overflows per kind (amount = diff - capacity, percentiles over all events):"
    );
    println!(
        "  {:<14} {:>10} | {:>8} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "kind", "count", "min", "p50", "p95", "p99", "max", "mean"
    );
    for r in rows {
        println!(
            "  {:<14} {:>10} | {:>8} {:>8} {:>8} {:>8} {:>8} {:>10.1}",
            sketch_kind_from_u8(r.kind).as_label(),
            r.event_count,
            r.min,
            r.p50,
            r.p95,
            r.p99,
            r.max,
            r.mean
        );
    }
    Ok(())
}

// ---- top-N overflow pairs ------------------------------------------

#[derive(Debug)]
struct OverflowPair {
    receiver: u32,
    peer_label: String,
    kind: u8,
    count: i64,
    mean: f64,
    max: i64,
}

impl<'a> TryFrom<&Row<'a>> for OverflowPair {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            receiver: row.get(0)?,
            peer_label: row.get(1)?,
            kind: row.get(2)?,
            count: row.get(3)?,
            mean: row.get::<usize, f64>(4).unwrap_or(0.0),
            max: row.get(5).unwrap_or(0),
        })
    }
}

fn print_top_overflow_pairs(conn: &Connection, top_n: usize) -> Result<()> {
    let sql = format!(
        "SELECT e.receiver_idx,
                COALESCE(p.pubkey, '0x' || printf('%016x', e.peer_id)) AS peer_label,
                e.kind, COUNT(*) AS cnt, AVG(e.amount), MAX(e.amount)
         FROM overflow_events e
         LEFT JOIN node_pubkey p ON p.node_id = e.peer_id
         GROUP BY e.receiver_idx, peer_label, e.kind
         ORDER BY cnt DESC LIMIT {top_n}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows: Vec<OverflowPair> = stmt
        .query_and_then([], |r| OverflowPair::try_from(r))?
        .collect::<Result<_, _>>()?;
    if rows.is_empty() {
        return Ok(());
    }
    println!("\ntop {top_n} overflow pairs (receiver, peer, kind, count, mean_amount):");
    for r in rows {
        println!(
            "  receiver=n{:<6} peer={} kind={} count={:>4} mean={:>6.1} max={}",
            r.receiver,
            r.peer_label,
            sketch_kind_from_u8(r.kind).as_label(),
            r.count,
            r.mean,
            r.max
        );
    }
    Ok(())
}

// ---- per-message convergence table ---------------------------------

fn percentile_columns(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = 'msgs' AND column_name LIKE 'p%_ns'
         ORDER BY ordinal_position",
    )?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(cols)
}

fn print_per_message_table(conn: &Connection, top_n: usize) -> Result<()> {
    let cols = percentile_columns(conn)?;
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM msgs", [], |r| r.get(0))?;
    if total == 0 {
        return Ok(());
    }
    let n_nodes: i64 = conn
        .query_row("SELECT n_nodes FROM run_meta", [], |r| r.get(0))
        .unwrap_or(0);
    let cols_sql = cols.join(", ");
    let sql = format!(
        "SELECT msg_id, coverage, {cols_sql} FROM msgs ORDER BY msg_id LIMIT {top_n}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    println!(
        "\nper-message convergence (showing first {top_n} of {total}, n_nodes={n_nodes}):"
    );
    let header_pcts: Vec<String> = cols.iter().map(|c| c.trim_end_matches("_ns").to_string()).collect();
    println!(
        "  {:<18} {:>7} {:>9}  {}",
        "msg",
        "covg",
        "covg%",
        header_pcts.iter().map(|s| format!("{s:>9}")).collect::<Vec<_>>().join(" ")
    );
    while let Some(r) = rows.next()? {
        let msg_id: u64 = r.get(0)?;
        let coverage: u64 = r.get(1)?;
        let cov_pct = if n_nodes > 0 {
            100.0 * coverage as f64 / n_nodes as f64
        } else {
            0.0
        };
        let cells: Vec<String> = (0..cols.len())
            .map(|i| match r.get::<_, Option<u64>>(2 + i).ok().flatten() {
                Some(ns) => fmt_dur_ns(ns),
                None => format!("{:>9}", "--"),
            })
            .collect();
        println!(
            "  0x{msg_id:016x} {coverage:>7} {cov_pct:>8.1}%  {}",
            cells.join(" ")
        );
    }
    Ok(())
}

// ---- coverage distribution -----------------------------------------

fn print_coverage_distribution(conn: &Connection) -> Result<()> {
    let n_nodes: i64 = conn
        .query_row("SELECT n_nodes FROM run_meta", [], |r| r.get(0))
        .unwrap_or(0);
    if n_nodes == 0 {
        return Ok(());
    }
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM msgs", [], |r| r.get(0))?;
    if total == 0 {
        return Ok(());
    }
    println!("\ncoverage distribution (messages reaching >= X% of {n_nodes} nodes):");
    for tier in crate::spread_model::COVERAGE_TIERS {
        let target = ((tier * n_nodes as f64).ceil() as i64).max(1);
        let hit: i64 = conn.query_row(
            "SELECT COUNT(*) FROM msgs WHERE coverage >= ?",
            [target],
            |r| r.get(0),
        )?;
        let pct = 100.0 * hit as f64 / total as f64;
        println!(
            "  >= {:>3.0}% (>= {:>5} nodes): {:>6} / {} ({:.1}%)",
            tier * 100.0,
            target,
            hit,
            total,
            pct
        );
    }
    Ok(())
}

// ---- per-tier convergence distribution -----------------------------

#[derive(Debug, Default)]
struct TierDistribution {
    min: u64,
    p05: u64,
    p25: u64,
    p50: u64,
    p75: u64,
    p95: u64,
    mean: u64,
    max: u64,
    n_msgs: i64,
}

impl<'a> TryFrom<&Row<'a>> for TierDistribution {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            min: u64_from_f64(row, 0),
            p05: u64_from_f64(row, 1),
            p25: u64_from_f64(row, 2),
            p50: u64_from_f64(row, 3),
            p75: u64_from_f64(row, 4),
            p95: u64_from_f64(row, 5),
            mean: u64_from_f64(row, 6),
            max: u64_from_f64(row, 7),
            n_msgs: row.get(8)?,
        })
    }
}

fn print_per_tier_distribution(conn: &Connection) -> Result<()> {
    let n_nodes: i64 = conn
        .query_row("SELECT n_nodes FROM run_meta", [], |r| r.get(0))
        .unwrap_or(0);
    if n_nodes == 0 {
        return Ok(());
    }
    let cols = percentile_columns(conn)?;
    for (tier, prefix) in [
        (0.05, "p5"),
        (0.25, "p25"),
        (0.50, "p50"),
        (0.75, "p75"),
        (0.95, "p95"),
        (0.99, "p99"),
        (1.00, "p100"),
    ] {
        let col = match cols.iter().find(|c| c.starts_with(&format!("{prefix}_"))) {
            Some(c) => c,
            None => continue,
        };
        let target = ((tier * n_nodes as f64).ceil() as i64).max(1);
        let sql = format!(
            "SELECT MIN({col}),
                    quantile_cont({col}, 0.05), quantile_cont({col}, 0.25),
                    quantile_cont({col}, 0.50), quantile_cont({col}, 0.75),
                    quantile_cont({col}, 0.95),
                    AVG({col}), MAX({col}), COUNT({col})
             FROM msgs WHERE {col} IS NOT NULL"
        );
        let mut stmt = conn.prepare(&sql)?;
        let d: TierDistribution = match stmt
            .query_and_then([], |r| TierDistribution::try_from(r))?
            .next()
        {
            Some(r) => r?,
            None => continue,
        };
        if d.n_msgs == 0 {
            continue;
        }
        println!(
            "\ntime to reach {:.0}% coverage (>= {} of {n_nodes} nodes): {} messages reached it",
            tier * 100.0,
            target,
            d.n_msgs
        );
        println!("  min:    {}", fmt_dur_ns(d.min));
        println!("  p  5:   {}", fmt_dur_ns(d.p05));
        println!("  p 25:   {}", fmt_dur_ns(d.p25));
        println!("  p 50:   {}", fmt_dur_ns(d.p50));
        println!("  p 75:   {}", fmt_dur_ns(d.p75));
        println!("  p 95:   {}", fmt_dur_ns(d.p95));
        println!("  mean:   {}", fmt_dur_ns(d.mean));
        println!("  max:    {}", fmt_dur_ns(d.max));
    }
    Ok(())
}

// ---- helpers --------------------------------------------------------

fn sketch_kind_from_u8(k: u8) -> SketchKind {
    match k {
        0 => SketchKind::ChanUpdates,
        1 => SketchKind::NodeAnns,
        2 => SketchKind::ChanAnns,
        _ => SketchKind::ChanUpdates,
    }
}
