//! `lnconv` CLI — load a TOML config, run the simulation, print
//! per-message percentile stats. All real logic lives in `lnconv-core`;
//! this binary is a thin clap + reporting wrapper.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use lnconv_core::config::SimConfig;
use lnconv_core::metrics::MsgStats;
use lnconv_core::sim;

#[derive(Parser, Debug)]
#[command(name = "lnconv", version, about = "LN gossip simulator")]
struct Cli {
    /// Path to the TOML simulation config.
    #[arg(short, long)]
    config: PathBuf,
}

/// Per-message percentiles: time at which X% of *all* nodes had received
/// the message. Each one shows up as a column in the per-message table
/// AND as a row in the per-coverage-tier aggregate.
const PERCENTILES: &[f64] = &[0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99, 1.00];

/// Coverage tiers used to bucket messages for aggregate reports.
/// "messages with coverage >= 25% of n_nodes" gets one bucket, etc.
const COVERAGE_TIERS: &[f64] = &[0.25, 0.50, 0.75, 1.00];

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = SimConfig::from_path(&cli.config)?;
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

    if stats.is_empty() {
        return Ok(());
    }

    print_per_message_table(&stats, n);
    print_coverage_distribution(&stats, n);
    print_per_tier_distribution(&stats, n);

    Ok(())
}

/// Print up to 5 per-message rows showing absolute-coverage percentiles.
/// `--` denotes a percentile the message never reached.
fn print_per_message_table(stats: &[MsgStats], n: usize) {
    let header_pcts: Vec<String> = PERCENTILES
        .iter()
        .map(|p| format!("p{:>3.0}", p * 100.0))
        .collect();
    println!(
        "  {:<6} {:>7} {:>9}  {}",
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
            "  {:<6} {:>7} {:>8.1}%  {}",
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
