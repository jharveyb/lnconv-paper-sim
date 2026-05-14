//! `lnconv` CLI — thin clap wrapper. All simulation logic lives in
//! `lnconv-core`; this binary parses TOML, kicks off the sim, then
//! delegates end-of-run reporting to `lnconv_core::duckdb_report`,
//! which runs canned DuckDB SQL over the six Parquet files written by
//! the sim's `stats_writer`.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use lnconv_core::config::SimConfig;
use lnconv_core::{duckdb_report, sim};

/// Per-message percentiles emitted to `msg_stats.parquet`. Adding a
/// percentile here adds a column to that file; `duckdb_report` picks
/// them up automatically by introspecting the schema.
const PERCENTILES: &[f64] = &[0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.00];

#[derive(Parser, Debug)]
#[command(name = "lnconv", version, about = "LN gossip simulator")]
struct Cli {
    /// Path to the TOML simulation config.
    #[arg(short, long)]
    config: PathBuf,
    /// Override the executor's worker-thread count.
    #[arg(short, long)]
    threads: Option<usize>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut cfg = SimConfig::from_path(&cli.config)?;
    if let Some(n) = cli.threads {
        cfg.run.threads = Some(n);
    }
    println!("config: {cfg:#?}");

    let result = sim::run(&cfg, PERCENTILES.to_vec())?;
    let tag = result.metrics.tag_prefix();
    println!(
        "simulation finished: {} distinct messages",
        result.metrics.completed_count()
    );
    println!(
        "total first-seen events: {}",
        result.metrics.total_first_seen()
    );
    let superseded = result.metrics.superseded_count();
    if superseded > 0 {
        println!("superseded: {superseded} messages killed mid-spread by a newer version");
    }

    // Drop the metrics handle to close the stats writer thread (flushes
    // the final Parquet rows + row group). Necessary before DuckDB
    // opens the files.
    drop(result);

    if let Some(tag) = tag {
        duckdb_report::run_summary_report(&tag)?;
    } else {
        eprintln!("no stats tag prefix on metrics handle — no DuckDB report to run");
    }
    Ok(())
}
