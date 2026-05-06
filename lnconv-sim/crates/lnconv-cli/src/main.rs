//! `lnconv` CLI — load a TOML config, run the simulation, print
//! per-message percentile stats. All real logic lives in `lnconv-core`;
//! this binary is a thin clap + reporting wrapper.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use lnconv_core::config::SimConfig;
use lnconv_core::sim;

#[derive(Parser, Debug)]
#[command(name = "lnconv", version, about = "LN gossip simulator")]
struct Cli {
    /// Path to the TOML simulation config.
    #[arg(short, long)]
    config: PathBuf,
}

const PERCENTILES: &[f64] = &[0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99, 1.00];

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = SimConfig::from_path(&cli.config)?;
    println!("config: {cfg:#?}");

    let result = sim::run(&cfg)?;
    let n = result.topology.len();

    let stats = result.metrics.per_message_stats(PERCENTILES);
    println!("simulation finished: {} distinct messages", stats.len());
    println!(
        "total first-seen events: {}",
        result.metrics.total_first_seen()
    );

    if stats.is_empty() {
        return Ok(());
    }

    // Per-message detail: print up to a few rows, then aggregate.
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
            .map(|(_p, d)| fmt_dur(*d))
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

    // Aggregate (mean) percentile times across all messages that hit 100%
    // coverage — these are the comparable runs.
    let full_cov: Vec<_> = stats.iter().filter(|s| s.coverage == n).collect();
    if !full_cov.is_empty() {
        println!(
            "\naggregate over {} messages that reached 100% coverage:",
            full_cov.len()
        );
        for (i, &p) in PERCENTILES.iter().enumerate() {
            let mean_ns: f64 = full_cov
                .iter()
                .map(|s| s.percentiles[i].1.as_nanos() as f64)
                .sum::<f64>()
                / full_cov.len() as f64;
            let mean_dur = std::time::Duration::from_nanos(mean_ns as u64);
            println!("  p{:>3.0}: mean = {}", p * 100.0, fmt_dur(mean_dur));
        }
    }

    Ok(())
}

fn fmt_dur(d: std::time::Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:>7.0}μs", d.as_micros() as f64)
    } else if ms < 1000.0 {
        format!("{ms:>8.1}ms")
    } else {
        format!("{:>8.2}s ", d.as_secs_f64())
    }
}
