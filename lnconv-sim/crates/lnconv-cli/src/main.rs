//! `lnconv` CLI — load a TOML config, run the simulation, print
//! per-message percentile stats. All real logic lives in `lnconv-core`;
//! this binary is a thin clap + reporting wrapper.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use lnconv_core::config::SimConfig;
use lnconv_core::metrics::{MsgStats, NodeSummary};
use lnconv_core::sim;

#[derive(Parser, Debug)]
#[command(name = "lnconv", version, about = "LN gossip simulator")]
struct Cli {
    /// Path to the TOML simulation config.
    #[arg(short, long)]
    config: PathBuf,
    /// Override the executor's worker-thread count. If unset, uses
    /// `[run].threads` from the config (or NeXosim's default — all
    /// logical cores — when neither is set).
    #[arg(short, long)]
    threads: Option<usize>,
}

/// Per-message percentiles: time at which X% of *all* nodes had received
/// the message. Each one shows up as a column in the per-message table
/// AND as a row in the per-coverage-tier aggregate.
const PERCENTILES: &[f64] = &[0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.00];

/// Coverage tiers used to bucket messages for aggregate reports.
/// "messages with coverage >= 25% of n_nodes" gets one bucket, etc.
const COVERAGE_TIERS: &[f64] = &[0.05, 0.25, 0.50, 0.75, 0.95, 0.99, 1.00];

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut cfg = SimConfig::from_path(&cli.config)?;
    if let Some(n) = cli.threads {
        cfg.run.threads = Some(n);
    }
    println!("config: {cfg:#?}");

    let result = sim::run(&cfg, PERCENTILES.to_vec())?;
    let n = result.topology.len();

    let stats = result.metrics.completed_stats();
    println!("simulation finished: {} distinct messages", stats.len());
    println!(
        "total first-seen events: {}",
        result.metrics.total_first_seen()
    );
    let superseded = result.metrics.superseded_count();
    if superseded > 0 {
        println!(
            "superseded: {superseded} of {} messages were killed mid-spread by a newer (scid, direction) version",
            stats.len()
        );
    }

    let per_node = result.metrics.per_node_summary();
    print_per_node_summary(&per_node);
    print_sketch_summary(&per_node);

    if stats.is_empty() {
        return Ok(());
    }

    print_per_message_table(&stats, n);
    print_coverage_distribution(&stats, n);
    print_per_tier_distribution(&stats, n);

    Ok(())
}

/// Five-number summary helper for u64 columns.
fn fivenum(mut xs: Vec<u64>) -> (u64, u64, u64, u64, u64, u64) {
    if xs.is_empty() {
        return (0, 0, 0, 0, 0, 0);
    }
    xs.sort_unstable();
    let total: u64 = xs.iter().sum();
    let pct = |p: f64| -> u64 {
        let i = ((xs.len() as f64) * p).ceil() as usize;
        let i = i.saturating_sub(1).min(xs.len() - 1);
        xs[i]
    };
    (xs[0], pct(0.50), pct(0.95), *xs.last().unwrap(), total, xs.len() as u64)
}

fn print_per_node_summary(per_node: &[NodeSummary]) {
    if per_node.is_empty() {
        return;
    }
    let bytes_in: Vec<u64> = per_node.iter().map(|s| s.bytes_in).collect();
    let bytes_out: Vec<u64> = per_node.iter().map(|s| s.bytes_out).collect();
    let dups: Vec<u64> = per_node.iter().map(|s| s.duplicates).collect();
    let (bi_min, bi_p50, bi_p95, bi_max, bi_total, n) = fivenum(bytes_in);
    let (bo_min, bo_p50, bo_p95, bo_max, bo_total, _) = fivenum(bytes_out);
    let (du_min, du_p50, du_p95, du_max, du_total, _) = fivenum(dups);
    println!("\nper-node bandwidth + duplicates (n={n}):");
    println!("  bytes_in:    min={bi_min:>10}  p50={bi_p50:>10}  p95={bi_p95:>10}  max={bi_max:>10}  total={bi_total}");
    println!("  bytes_out:   min={bo_min:>10}  p50={bo_p50:>10}  p95={bo_p95:>10}  max={bo_max:>10}  total={bo_total}");
    println!("  duplicates:  min={du_min:>10}  p50={du_p50:>10}  p95={du_p95:>10}  max={du_max:>10}  total={du_total}");
}

fn print_sketch_summary(per_node: &[NodeSummary]) {
    let sent: u64 = per_node.iter().map(|s| s.sketches_sent).sum();
    let received: u64 = per_node.iter().map(|s| s.sketches_received).sum();
    if sent == 0 && received == 0 {
        return; // no sketch traffic — don't clutter the output
    }
    let overflowed: u64 = per_node.iter().map(|s| s.sketches_overflowed).sum();
    let inter: u64 = per_node.iter().map(|s| s.intersection_total).sum();
    let a_only: u64 = per_node.iter().map(|s| s.a_only_total).sum();
    let b_only: u64 = per_node.iter().map(|s| s.b_only_total).sum();
    let mean_inter = if received > 0 { inter as f64 / received as f64 } else { 0.0 };
    let mean_a = if received > 0 { a_only as f64 / received as f64 } else { 0.0 };
    let mean_b = if received > 0 { b_only as f64 / received as f64 } else { 0.0 };
    let overflow_pct = if received > 0 { 100.0 * overflowed as f64 / received as f64 } else { 0.0 };
    println!("\nsketch protocol:");
    println!("  sent / received: {sent} / {received}");
    println!("  overflows (diff > capacity): {overflowed} ({overflow_pct:.1}%)");
    println!(
        "  mean intersection: {mean_inter:.1}  mean a_only: {mean_a:.1}  mean b_only: {mean_b:.1}"
    );
}

/// Print up to 5 per-message rows showing absolute-coverage percentiles.
/// `--` denotes a percentile the message never reached.
fn print_per_message_table(stats: &[MsgStats], n: usize) {
    let header_pcts: Vec<String> = PERCENTILES
        .iter()
        .map(|p| format!("p{:>3.0}", p * 100.0))
        .collect();
    // MsgId is now a 64-bit content-derived hash; print as
    // 16-hex with "0x" prefix so it fits a fixed 18-char column.
    println!(
        "  {:<18} {:>7} {:>9}  {}",
        "msg",
        "covg",
        "covg%",
        header_pcts.join("    ")
    );
    let to_show = stats.len().min(5);
    for s in &stats[..to_show] {
        let cov_pct = 100.0 * s.coverage as f64 / n as f64;
        let cells: Vec<String> = s
            .percentiles
            .iter()
            .map(|(_p, d)| fmt_opt_dur(*d))
            .collect();
        println!(
            "  0x{:016x} {:>7} {:>8.1}%  {}",
            s.id,
            s.coverage,
            cov_pct,
            cells.join("  ")
        );
    }
    if stats.len() > to_show {
        println!("  ... ({} more messages)", stats.len() - to_show);
    }
}

/// "How many messages reached at least X% of nodes?" — one count per
/// coverage tier. Helpful when supersession is killing some messages
/// mid-spread; for runs where everything converges this row is all
/// `M / M (100.0%)`.
fn print_coverage_distribution(stats: &[MsgStats], n: usize) {
    println!("\ncoverage distribution (messages reaching >= X% of {n} nodes):");
    let total = stats.len();
    for &tier in COVERAGE_TIERS {
        let target = (tier * n as f64).ceil() as usize;
        let target = target.max(1);
        let hit = stats.iter().filter(|s| s.coverage >= target).count();
        let pct = 100.0 * hit as f64 / total.max(1) as f64;
        println!(
            "  >= {:>3.0}% (>= {:>5} nodes): {:>6} / {} ({:.1}%)",
            tier * 100.0,
            target,
            hit,
            total,
            pct,
        );
    }
}

/// For each coverage tier, the *distribution across messages* of the
/// time it took a message to reach that coverage. This answers the
/// question "what kind of times do messages typically take to reach
/// X% of the network?" — distinct from "for an average message, what's
/// the time at which X% of nodes saw it" (which is the previous,
/// confusingly-named per-percentile mean).
///
/// Each table reports the time-to-reach-tier values from min through
/// max with a few percentiles in between. Comparing tables lets you
/// see how much extra time the wave needs to grow from 25% to 100%.
fn print_per_tier_distribution(stats: &[MsgStats], n: usize) {
    // Tier names map onto entries of `PERCENTILES` so we can reuse the
    // already-computed per-message values without re-collecting times.
    let pct_idx = |tier: f64| -> usize {
        PERCENTILES
            .iter()
            .position(|&p| (p - tier).abs() < 1e-9)
            .expect("COVERAGE_TIERS must be a subset of PERCENTILES")
    };

    for &tier in COVERAGE_TIERS {
        let target = ((tier * n as f64).ceil() as usize).max(1);
        let i = pct_idx(tier);

        // Time-to-reach-tier for each message that hit it.
        let mut times_ns: Vec<u128> = stats
            .iter()
            .filter_map(|s| s.percentiles[i].1.map(|d| d.as_nanos()))
            .collect();
        if times_ns.is_empty() {
            continue;
        }
        times_ns.sort_unstable();

        let count = times_ns.len();
        let mean_ns = times_ns.iter().sum::<u128>() as f64 / count as f64;
        let pct_at = |p: f64| -> Duration {
            let idx = ((p * count as f64).ceil() as isize - 1).max(0) as usize;
            Duration::from_nanos(times_ns[idx.min(count - 1)] as u64)
        };

        println!(
            "\ntime to reach {:.0}% coverage (>= {} of {n} nodes): {} of {} messages reached it",
            tier * 100.0,
            target,
            count,
            stats.len(),
        );
        println!("  min:    {}", fmt_dur(Duration::from_nanos(times_ns[0] as u64)));
        for &dp in &[0.05_f64, 0.25, 0.50, 0.75, 0.95] {
            println!("  p{:>3.0}:   {}", dp * 100.0, fmt_dur(pct_at(dp)));
        }
        println!("  mean:   {}", fmt_dur(Duration::from_nanos(mean_ns as u64)));
        println!(
            "  max:    {}",
            fmt_dur(Duration::from_nanos(*times_ns.last().unwrap() as u64))
        );
    }
}

fn fmt_dur(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:>7.0}μs", d.as_micros() as f64)
    } else if ms < 1000.0 {
        format!("{ms:>8.1}ms")
    } else {
        format!("{:>8.2}s ", d.as_secs_f64())
    }
}

fn fmt_opt_dur(d: Option<Duration>) -> String {
    match d {
        Some(d) => fmt_dur(d),
        None => format!("{:>9}", "--"),
    }
}
