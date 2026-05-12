//! `lnconv` CLI — load a TOML config, run the simulation, print
//! per-message percentile stats. All real logic lives in `lnconv-core`;
//! this binary is a thin clap + reporting wrapper.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use lnconv_core::config::{SimConfig, TopologyCfg};
use lnconv_core::message::{NodeId, SketchKind};
use lnconv_core::metrics::{MsgStats, NodeSummary, SketchKindStats};
use lnconv_core::sim;
use lnconv_core::topology::ln_data;

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
    let pubkey_lookup = build_pubkey_lookup(&cfg);
    print_sketch_summary(&per_node, &pubkey_lookup);

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
    // Split bandwidth into sketch vs gossip buckets. Sketch traffic
    // only appears on sketch nodes; gossip traffic on everything.
    let bi_gossip: Vec<u64> = per_node.iter().map(|s| s.bytes_in_gossip).collect();
    let bo_gossip: Vec<u64> = per_node.iter().map(|s| s.bytes_out_gossip).collect();
    let bi_sketch: Vec<u64> = per_node.iter().map(|s| s.bytes_in_sketch).collect();
    let bo_sketch: Vec<u64> = per_node.iter().map(|s| s.bytes_out_sketch).collect();
    let dups: Vec<u64> = per_node.iter().map(|s| s.duplicates).collect();
    let any_sketch = bi_sketch.iter().any(|&v| v > 0) || bo_sketch.iter().any(|&v| v > 0);

    let (big_min, big_p50, big_p95, big_max, big_total, n) = fivenum(bi_gossip);
    let (bog_min, bog_p50, bog_p95, bog_max, bog_total, _) = fivenum(bo_gossip);
    let (du_min, du_p50, du_p95, du_max, du_total, _) = fivenum(dups);
    println!("\nper-node bandwidth + duplicates (n={n}):");
    println!("  bytes_in (gossip):   min={big_min:>10}  p50={big_p50:>10}  p95={big_p95:>10}  max={big_max:>10}  total={big_total}");
    println!("  bytes_out (gossip):  min={bog_min:>10}  p50={bog_p50:>10}  p95={bog_p95:>10}  max={bog_max:>10}  total={bog_total}");
    if any_sketch {
        let (bis_min, bis_p50, bis_p95, bis_max, bis_total, _) = fivenum(bi_sketch);
        let (bos_min, bos_p50, bos_p95, bos_max, bos_total, _) = fivenum(bo_sketch);
        println!("  bytes_in (sketch):   min={bis_min:>10}  p50={bis_p50:>10}  p95={bis_p95:>10}  max={bis_max:>10}  total={bis_total}");
        println!("  bytes_out (sketch):  min={bos_min:>10}  p50={bos_p50:>10}  p95={bos_p95:>10}  max={bos_max:>10}  total={bos_total}");
    }
    println!("  duplicates:          min={du_min:>10}  p50={du_p50:>10}  p95={du_p95:>10}  max={du_max:>10}  total={du_total}");
}

/// Pull out the `SketchKindStats` for a given kind on a NodeSummary.
fn kind_stats<'a>(s: &'a NodeSummary, k: SketchKind) -> &'a SketchKindStats {
    match k {
        SketchKind::ChanUpdates => &s.chan_updates_stats,
        SketchKind::NodeAnns => &s.node_anns_stats,
        SketchKind::ChanAnns => &s.chan_anns_stats,
    }
}

fn overflow_count_for_kind(s: &NodeSummary, k: SketchKind) -> u64 {
    match k {
        SketchKind::ChanUpdates => s.overflowed_chan_updates,
        SketchKind::NodeAnns => s.overflowed_node_anns,
        SketchKind::ChanAnns => s.overflowed_chan_anns,
    }
}

fn kind_label(k: SketchKind) -> &'static str {
    match k {
        SketchKind::ChanUpdates => "chan_updates",
        SketchKind::NodeAnns => "node_anns   ",
        SketchKind::ChanAnns => "chan_anns   ",
    }
}

/// Build the reverse-lookup from hashed `NodeId` to original 66-hex
/// pubkey for CSV-loaded topologies. Empty map for synthetic
/// topologies — their NodeIds are dense `0..n` and render fine as
/// integers; the overflow-pair printer just falls back to the hex
/// form when the lookup is empty.
fn build_pubkey_lookup(cfg: &SimConfig) -> HashMap<NodeId, String> {
    match &cfg.topology {
        TopologyCfg::FromCsv { nodes_csv, .. } => {
            match ln_data::read_pubkey_lookup(nodes_csv, cfg.seed) {
                Ok(map) => map,
                Err(e) => {
                    eprintln!("warning: pubkey lookup unavailable: {e}");
                    HashMap::new()
                }
            }
        }
        _ => HashMap::new(),
    }
}

fn print_sketch_summary(per_node: &[NodeSummary], pubkey_lookup: &HashMap<NodeId, String>) {
    let sent: u64 = per_node.iter().map(|s| s.sketches_sent).sum();
    let received: u64 = per_node.iter().map(|s| s.sketches_received).sum();
    if sent == 0 && received == 0 {
        return; // no sketch traffic — don't clutter the output
    }
    let total_overflowed: u64 = per_node
        .iter()
        .map(|s| s.overflowed_chan_updates + s.overflowed_node_anns + s.overflowed_chan_anns)
        .sum();
    let overflow_pct = if received > 0 { 100.0 * total_overflowed as f64 / received as f64 } else { 0.0 };
    println!("\nsketch protocol:");
    println!("  sent / received: {sent} / {received}");
    println!("  overflows (diff > capacity): {total_overflowed} ({overflow_pct:.1}%)");

    // Per-node sketch send/recv distribution. The totals above hide
    // load skew between hub-like and leaf-like nodes; this shows the
    // shape (e.g. how many sketches did the most-overloaded receiver
    // process).
    let sent_series: Vec<u64> = per_node.iter().map(|s| s.sketches_sent).collect();
    let recv_series: Vec<u64> = per_node.iter().map(|s| s.sketches_received).collect();
    let (ss_mn, ss_p50, ss_p95, ss_mx, ss_total, ss_n) = fivenum(sent_series);
    let (sr_mn, sr_p50, sr_p95, sr_mx, sr_total, _) = fivenum(recv_series);
    let ss_mean = if ss_n > 0 { ss_total as f64 / ss_n as f64 } else { 0.0 };
    let sr_mean = if ss_n > 0 { sr_total as f64 / ss_n as f64 } else { 0.0 };
    println!("  per-node distribution (n={ss_n}):");
    println!(
        "    sketches_sent:     min={ss_mn:>6}  p50={ss_p50:>6}  p95={ss_p95:>6}  max={ss_mx:>6}  mean={ss_mean:>8.1}"
    );
    println!(
        "    sketches_received: min={sr_mn:>6}  p50={sr_p50:>6}  p95={sr_p95:>6}  max={sr_mx:>6}  mean={sr_mean:>8.1}"
    );

    // Per-kind reconciliation breakdown. For each kind × counter we
    // run `fivenum` on the per-node series so the CLI shows the
    // distribution of where the reconciliation work landed (per-node
    // min/p50/p95/max/mean), followed by the network-wide sum.
    const KINDS: [SketchKind; 3] = [SketchKind::ChanUpdates, SketchKind::NodeAnns, SketchKind::ChanAnns];
    println!("\nsketch reconciliation per kind (per-node min/p50/p95/max/mean; total = network sum):");
    println!(
        "  {:<14} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>12} {:>16}",
        "kind", "counter", "min", "p50", "p95", "max", "mean", "total (net)"
    );
    for k in KINDS {
        let getters: [(&str, fn(&SketchKindStats) -> u64); 3] = [
            ("intersection", |st| st.intersection),
            ("a_only", |st| st.a_only),
            ("b_only", |st| st.b_only),
        ];
        for (label, getter) in getters.iter() {
            let vals: Vec<u64> = per_node.iter().map(|s| getter(kind_stats(s, k))).collect();
            let (mn, p50, p95, mx, total, n) = fivenum(vals);
            let mean = if n > 0 { total as f64 / n as f64 } else { 0.0 };
            println!(
                "  {} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>12.1} {:>16}",
                kind_label(k),
                label,
                mn,
                p50,
                p95,
                mx,
                mean,
                total
            );
        }
    }

    // Per-reconciliation-round distribution. Same kind × counter
    // matrix but the percentiles are taken across individual rounds
    // (one round = one `handle_sketch` call). The `total_diff` row
    // (a_only + b_only per round) is the load that a sketch's
    // capacity must absorb — its p99 is a useful lower bound on the
    // capacity needed to keep overflows rare.
    println!("\nsketch reconciliation per kind (per-round distribution):");
    println!(
        "  {:<14} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "kind", "counter", "n_rounds", "min", "p50", "p95", "p99", "max", "mean"
    );
    for k in KINDS {
        // Concatenate per-kind round samples across all nodes.
        let mut intersections: Vec<u64> = Vec::new();
        let mut a_onlys: Vec<u64> = Vec::new();
        let mut b_onlys: Vec<u64> = Vec::new();
        let mut totals: Vec<u64> = Vec::new();
        for s in per_node {
            let st = kind_stats(s, k);
            let inter_s = st.rounds_intersection.samples();
            let a_s = st.rounds_a_only.samples();
            let b_s = st.rounds_b_only.samples();
            let n = inter_s.len().min(a_s.len()).min(b_s.len());
            for i in 0..n {
                let inter = inter_s[i] as u64;
                let a = a_s[i] as u64;
                let b = b_s[i] as u64;
                intersections.push(inter);
                a_onlys.push(a);
                b_onlys.push(b);
                totals.push(a + b);
            }
        }
        if intersections.is_empty() {
            continue;
        }
        let rows: [(&str, Vec<u64>); 4] = [
            ("intersection", intersections),
            ("a_only", a_onlys),
            ("b_only", b_onlys),
            ("total_diff", totals),
        ];
        for (label, mut vals) in rows {
            vals.sort_unstable();
            let n = vals.len();
            let pct = |p: f64| -> u64 {
                let i = ((n as f64) * p).ceil() as usize;
                vals[i.saturating_sub(1).min(n - 1)]
            };
            let total: u64 = vals.iter().sum();
            let mean = total as f64 / n as f64;
            println!(
                "  {} {:>11} | {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10.1}",
                kind_label(k),
                label,
                n,
                vals[0],
                pct(0.50),
                pct(0.95),
                pct(0.99),
                vals[n - 1],
                mean
            );
        }
    }

    // Per-kind overflow counts (per-node distribution) + amount
    // percentiles taken over the flattened overflow_events list.
    println!("\nsketch overflows per kind:");
    println!(
        "  {:<14} {:>10} {:>10} {:>10} | amount (over all events)  min   p50   p95   max",
        "kind", "node_min", "node_p50", "node_max"
    );
    for k in KINDS {
        let counts: Vec<u64> = per_node.iter().map(|s| overflow_count_for_kind(s, k)).collect();
        let (cmn, cp50, _cp95, cmx, ctotal, _) = fivenum(counts);
        // Flatten amount events for this kind across all nodes.
        let mut amounts: Vec<u64> = Vec::new();
        for s in per_node {
            for ev in &s.overflow_events {
                if ev.kind == k {
                    amounts.push(ev.amount as u64);
                }
            }
        }
        let (amin, ap50, ap95, amax, _atotal, _) = fivenum(amounts);
        println!(
            "  {}  count: min={:>4} p50={:>4} max={:>5} total={:>6}    amount: min={:>5} p50={:>5} p95={:>6} max={:>7}",
            kind_label(k),
            cmn,
            cp50,
            cmx,
            ctotal,
            amin,
            ap50,
            ap95,
            amax
        );
    }

    // Top-N (receiver, peer) overflow pairs by count.
    print_top_overflow_pairs(per_node, 10, pubkey_lookup);
}

/// Print the top-N (receiver, peer) pairs by overflow count. Reaches
/// into each NodeSummary's `overflow_events` list and tallies by
/// (receiver_idx, peer_id). When `pubkey_lookup` carries a mapping for
/// the peer's hashed NodeId (CSV-loaded snapshots), the peer column
/// renders the original 66-hex pubkey so it can be cross-referenced
/// against other LN data sources; otherwise it falls back to the raw
/// hex hash.
fn print_top_overflow_pairs(
    per_node: &[NodeSummary],
    top_n: usize,
    pubkey_lookup: &HashMap<NodeId, String>,
) {
    let mut tally: HashMap<(u32, u64), (u64, u64)> = HashMap::new(); // -> (count, sum_amount)
    for s in per_node {
        for ev in &s.overflow_events {
            let entry = tally.entry((s.idx, ev.peer_id)).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += ev.amount as u64;
        }
    }
    if tally.is_empty() {
        return;
    }
    let mut rows: Vec<((u32, u64), (u64, u64))> = tally.into_iter().collect();
    rows.sort_by(|a, b| b.1.0.cmp(&a.1.0)); // by count desc
    println!("\ntop {top_n} overflow pairs (receiver, peer, count, mean_amount):");
    for ((recv, peer), (count, sum_amount)) in rows.iter().take(top_n) {
        let mean = if *count > 0 { *sum_amount as f64 / *count as f64 } else { 0.0 };
        let peer_str = match pubkey_lookup.get(peer) {
            Some(pk) => pk.clone(),
            None => format!("0x{peer:016x}"),
        };
        println!(
            "  receiver=n{recv:<6} peer={peer_str}  count={count:>4}  mean_amount={mean:>6.1}"
        );
    }
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
