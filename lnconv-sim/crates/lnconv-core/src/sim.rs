//! Simulation orchestrator.
//!
//! Builds one of three things — a homogeneous flooding population, a
//! homogeneous stagger population (CLN or LND), or a mixed CLN+LND
//! population — wires up the inter-node connections according to the
//! topology, hands the result to NeXosim's executor, and drives time
//! forward in chunks while injecting scheduled originate-events and
//! emitting periodic progress lines.
//!
//! ## Why a chunked driver
//!
//! NeXosim's `Simulation::step_until(deadline)` happily advances all the
//! way to `deadline` in one call. That works when every event is already
//! in the queue at `t=0` (e.g. a one-shot run), but breaks two things we
//! want:
//!
//! 1. **Scheduled originations.** A Poisson stream produces events at
//!    `t=12.34s`, `t=12.92s`, ... Those have to be `process_event`'d into
//!    the simulation *at* that simulated time, not all at once at startup.
//! 2. **Live progress output.** Tens of millions of events can fire in
//!    one `step_until` call; we'd see a single block of output at the end.
//!
//! `drive_simulation` solves both by stepping to the next "checkpoint"
//! (the soonest of: deadline, next scheduled event, next progress tick),
//! then handling whatever became due.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use nexosim::ports::{EventSource, Output};
use nexosim::simulation::{EventId, Mailbox, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::channels::ChannelRegistry;
use crate::config::{AlgoCfg, EventCfg, LatencyCfg, NodeAlgoKind, SimConfig, TopologyCfg};
use crate::events::EventSchedule;
use crate::events::oneshot::{OneShotAll, OneShotSingle};
use crate::events::parquet_replay::ParquetReplay;
use crate::events::poisson::PoissonRandom;
use crate::message::{Gossip, GossipKind, NodeId};
use crate::metrics::MetricsHandle;
use crate::node::cln::ClnNode;
use crate::node::flooding::FloodingNode;
use crate::node::lnd::LndNode;
use crate::node::sketch::SketchNode;
use crate::state::{self, SharedNodeState};
use crate::topology::{NodeAlgo, Topology, metrics as topology_metrics, synthetic};

pub struct RunResult {
    pub metrics: MetricsHandle,
    pub topology: Topology,
}

/// Top-level entry point: build the topology, choose the right node
/// builder for the configured algorithm, kick off the chunked driver,
/// return the (still-locked) metrics for post-run reporting.
///
/// `percentiles` is the list of fractions (in [0.0, 1.0]) that
/// per-message convergence stats should be reported for; baked into the
/// metrics handle now so finalized summaries can be computed
/// incrementally as messages reach 100% coverage.
pub fn run(cfg: &SimConfig, percentiles: Vec<f64>) -> Result<RunResult> {
    // Topology build is split into three phases so per-vertex algo
    // can drive the per-vertex peer-build `k`:
    //   1. Vertices are added with `default_algo`.
    //   2. For Mix, per-vertex algos are replaced from `assign_mix`.
    //   3. Channels are loaded; the peer graph is built using each
    //      vertex's (now-final) algo to look up its `k`.
    //
    // KRegular's vertex degree is fixed at config.topology.k for ALL
    // vertices regardless of algo (true k-regular guarantee), so step
    // 3 is collapsed into step 1 for that variant.
    let default_algo = default_algo_from(&cfg.algo);
    let (mut topology, snap_for_csv) = match &cfg.topology {
        TopologyCfg::KRegular { n, k } => (
            synthetic::random_regular(*n, *k, cfg.seed, default_algo.clone()),
            None,
        ),
        TopologyCfg::FromCsv {
            nodes_csv,
            channels_csv,
            ..
        } => {
            if cfg.channels.is_some() {
                eprintln!(
                    "warning: [channels] block is ignored when topology.kind = \"from_csv\"; \
                     the CSV provides the channel set"
                );
            }
            let snap = crate::topology::ln_data::load(nodes_csv, channels_csv, cfg.seed)
                .map_err(|e| anyhow::anyhow!("CSV load failed: {e}"))?;
            println!(
                "loaded {} nodes, {} channels from CSVs",
                snap.nodes.len(),
                snap.channels.len()
            );
            let mut topo = Topology::with_capacity(snap.nodes.len());
            for &id in &snap.nodes {
                topo.add_node(id, default_algo.clone());
            }
            (topo, Some(snap))
        }
    };

    // Step 2: Mix per-vertex algo assignment. Must happen *before* the
    // FromCsv peer graph is built so per-impl `k` resolves correctly.
    let n = topology.len();
    if let AlgoCfg::Mix { population } = &cfg.algo {
        let assignments = assign_mix(n, population, cfg.seed);
        for (nx, kind) in topology.peers.node_indices().zip(assignments.iter()) {
            topology.peers[nx].algo = NodeAlgo::from(kind);
        }
        log_mix_summary(&assignments);
    }

    // Step 3: channels + peer graph (FromCsv only — KRegular finished in step 1).
    let registry = match &cfg.topology {
        TopologyCfg::KRegular { .. } => {
            let num_scids = cfg
                .channels
                .as_ref()
                .ok_or_else(|| {
                    anyhow::anyhow!("[channels] block is required for topology.kind = \"k_regular\"")
                })?
                .count;
            ChannelRegistry::build(&mut topology, num_scids, cfg.seed)
        }
        TopologyCfg::FromCsv {
            k,
            max_peer,
            enforce_hub_cap,
            ..
        } => {
            let snap = snap_for_csv.expect("snap is Some for FromCsv");
            let registry = ChannelRegistry::from_iter(&mut topology, snap.channels.iter().copied());
            let k_cln = k.cln;
            let k_lnd = k.lnd;
            let k_flooding = k.flooding;
            let k_sketch = k.sketch.unwrap_or(k.cln);
            let k_for = move |a: &NodeAlgo| match a {
                NodeAlgo::Flooding => k_flooding,
                NodeAlgo::Cln { .. } => k_cln,
                NodeAlgo::Lnd { .. } => k_lnd,
                NodeAlgo::Sketch { .. } => k_sketch,
            };
            let mp_cln = max_peer.cln;
            let mp_lnd = max_peer.lnd;
            let mp_flooding = max_peer.flooding;
            let mp_sketch = max_peer
                .sketch
                .unwrap_or_else(crate::config::default_max_peer_sketch);
            let max_peer_for = move |a: &NodeAlgo| match a {
                NodeAlgo::Flooding => mp_flooding,
                NodeAlgo::Cln { .. } => mp_cln,
                NodeAlgo::Lnd { .. } => mp_lnd,
                NodeAlgo::Sketch { .. } => mp_sketch,
            };
            crate::topology::synthetic::build_peer_graph(
                &mut topology,
                k_for,
                max_peer_for,
                cfg.seed,
                *enforce_hub_cap,
            );
            registry
        }
    };

    let topo_stats = topology_metrics::compute(&topology, cfg.seed, 2000, 1000);
    println!("topology: {topo_stats:#?}");

    println!(
        "channels: count={} (mean {:.1} per node)",
        registry.num_scids,
        registry.mean_per_node()
    );

    let stats_path = crate::stats_writer::auto_path(
        topology_kind_name(&cfg.topology),
        algo_kind_name(&cfg.algo),
        event_kind_name(&cfg.event),
    );
    println!("stats: streaming finalised MsgStats to {}", stats_path.display());
    let metrics = MetricsHandle::new(n, percentiles, Some(stats_path));
    let run_duration = Duration::from_secs(cfg.run.duration_seconds);

    // Snapshot the topology's NodeIds in NodeIdx order so event
    // streams can address nodes by vertex index (OneShotSingle) or
    // iterate them all (OneShotAll, PoissonRandom).
    let nodes_in_order: Vec<NodeId> = topology.node_ids().collect();
    let event_tuples = build_events(cfg, &nodes_in_order, run_duration, &registry)?;
    println!(
        "events: scheduled {} message(s) over the run window",
        event_tuples.len()
    );
    log_event_kind_breakdown(&event_tuples, run_duration);
    let deadline = MonotonicTime::EPOCH + run_duration;

    match &cfg.algo {
        AlgoCfg::Flooding {} => run_flooding(
            cfg,
            &topology,
            metrics.clone(),
            Duration::from_millis(latency_ms(&cfg.latency)),
            run_duration,
            event_tuples,
            deadline,
        )?,
        AlgoCfg::Cln { .. }
        | AlgoCfg::Lnd { .. }
        | AlgoCfg::Mix { .. }
        | AlgoCfg::Sketch { .. } => {
            run_stagger_population(
                cfg,
                &topology,
                metrics.clone(),
                run_duration,
                event_tuples,
                deadline,
            )?;
        }
    }

    // Convert any messages still in-flight at the deadline into final
    // stats with whatever partial coverage they reached.
    metrics.finalize_remaining();
    Ok(RunResult { metrics, topology })
}

/// Pick the per-vertex `NodeAlgo` to seed the topology with. For Mix
/// this is just a placeholder (Cln-with-zero-stagger) — the runner
/// overwrites every vertex's algo from `assign_mix` before any model is
/// constructed, so the placeholder is never observed downstream.
fn default_algo_from(cfg: &AlgoCfg) -> NodeAlgo {
    match cfg {
        AlgoCfg::Flooding {} => NodeAlgo::Flooding,
        AlgoCfg::Cln { stagger_ms } => NodeAlgo::Cln {
            stagger_ms: *stagger_ms,
        },
        AlgoCfg::Lnd {
            stagger_ms,
            trickle_ms,
            min_batch_size,
        } => NodeAlgo::Lnd {
            stagger_ms: *stagger_ms,
            trickle_ms: *trickle_ms,
            min_batch_size: *min_batch_size,
        },
        AlgoCfg::Mix { .. } => NodeAlgo::Cln { stagger_ms: 0 },
        AlgoCfg::Sketch {
            stagger_ms,
            capacity_chan_updates,
            capacity_node_anns,
            capacity_chan_anns,
        } => NodeAlgo::Sketch {
            stagger_ms: *stagger_ms,
            capacity_chan_updates: *capacity_chan_updates,
            capacity_node_anns: *capacity_node_anns,
            capacity_chan_anns: *capacity_chan_anns,
        },
    }
}

fn latency_ms(c: &LatencyCfg) -> u64 {
    match c {
        LatencyCfg::Constant { ms } => *ms,
    }
}

/// Build a `SimInit` with the executor thread count from `cfg.run.threads`.
///
/// `None` (the default) means "use NeXosim's default", which is all
/// available logical cores. `Some(n)` pins the worker pool to `n`. The CLI
/// `--threads` flag mutates `cfg.run.threads` before this is called, so a
/// CLI flag always wins.
fn new_sim_init(cfg: &SimConfig) -> SimInit {
    match cfg.run.threads {
        Some(n) if n > 0 => SimInit::with_num_threads(n),
        _ => SimInit::new(),
    }
}

fn build_events(
    cfg: &SimConfig,
    nodes: &[NodeId],
    max: Duration,
    registry: &ChannelRegistry,
) -> Result<Vec<(Duration, NodeId, Gossip)>> {
    let mut tuples = match &cfg.event {
        EventCfg::OneShotSingle { node } => OneShotSingle {
            node: *node,
            at: Duration::ZERO,
            size_bytes: 1024,
        }
        .build(nodes, max, registry),
        EventCfg::OneShotAll {} => OneShotAll {
            at: Duration::ZERO,
            size_bytes: 1024,
        }
        .build(nodes, max, registry),
        EventCfg::PoissonRandom {
            rate_per_sec,
            size_bytes,
        } => PoissonRandom {
            rate_per_sec: *rate_per_sec,
            seed: cfg.seed ^ 0xE7E,
            size_bytes: *size_bytes,
        }
        .build(nodes, max, registry),
        EventCfg::ParquetReplay { path } => {
            let paths = expand_glob(path)?;
            ParquetReplay {
                paths,
                seed: cfg.seed,
                snapshot_nodes: nodes.iter().copied().collect(),
            }
            .build(nodes, max, registry)
        }
    };
    tuples.sort_by_key(|(t, _, _)| *t);
    Ok(tuples)
}

/// Expand a glob pattern (or a literal path) to a sorted list of files.
/// Errors if zero matches — silently running with no events is rarely
/// the user's intent. Returned paths are in lexicographic order.
fn expand_glob(pat: &str) -> Result<Vec<std::path::PathBuf>> {
    let mut out: Vec<_> = glob::glob(pat)
        .map_err(|e| anyhow::anyhow!("bad glob pattern {pat:?}: {e}"))?
        .filter_map(std::result::Result::ok)
        .collect();
    if out.is_empty() {
        anyhow::bail!("no files matched pattern {pat:?}");
    }
    out.sort();
    Ok(out)
}

/// Deterministically partition `n` global node IDs across the entries of
/// `population`, with exact counts derived from the (normalized) fractions.
///
/// Uses largest-remainder rounding to ensure the per-kind counts sum to
/// exactly `n` regardless of how the floats divide. Then applies a single
/// shuffle so the kinds aren't all clumped together by node ID — wiring
/// across the topology then naturally produces mixed neighbourhoods.
fn assign_mix(n: usize, population: &[crate::config::MixEntry], seed: u64) -> Vec<NodeAlgoKind> {
    let total: f64 = population.iter().map(|m| m.fraction).sum();
    assert!(total > 0.0, "population fractions must be positive");

    let mut counts: Vec<usize> = population
        .iter()
        .map(|m| ((m.fraction / total) * n as f64).floor() as usize)
        .collect();
    let assigned: usize = counts.iter().sum();
    if assigned < n {
        let remainder = n - assigned;
        let mut frac_residuals: Vec<(usize, f64)> = population
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let exact = (m.fraction / total) * n as f64;
                (i, exact - exact.floor())
            })
            .collect();
        frac_residuals.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        for (i, _) in frac_residuals.iter().take(remainder) {
            counts[*i] += 1;
        }
    }

    let mut assignments: Vec<NodeAlgoKind> = Vec::with_capacity(n);
    for (i, m) in population.iter().enumerate() {
        for _ in 0..counts[i] {
            assignments.push(m.algo.clone());
        }
    }

    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xA55);
    assignments.shuffle(&mut rng);
    assignments
}

fn log_mix_summary(assignments: &[NodeAlgoKind]) {
    let n = assignments.len();
    let cln = assignments
        .iter()
        .filter(|a| matches!(a, NodeAlgoKind::Cln { .. }))
        .count();
    let lnd = assignments
        .iter()
        .filter(|a| matches!(a, NodeAlgoKind::Lnd { .. }))
        .count();
    println!(
        "population: cln={} ({:.1}%), lnd={} ({:.1}%)",
        cln,
        100.0 * cln as f64 / n as f64,
        lnd,
        100.0 * lnd as f64 / n as f64,
    );
}


/// Break down the scheduled event-tuples by `GossipKind` and print
/// counts + rates. Works for any event source — parquet replay sees a
/// mix of all three kinds, Poisson/OneShot are pure `ChannelUpdate`.
/// Rate is averaged over the full run window (`duration_seconds`),
/// which matches what the user reads off `[run]`.
fn log_event_kind_breakdown(tuples: &[(Duration, NodeId, Gossip)], window: Duration) {
    if tuples.is_empty() {
        return;
    }
    let mut chan_upd: u64 = 0;
    let mut node_ann: u64 = 0;
    let mut chan_ann: u64 = 0;
    for (_, _, g) in tuples {
        match g.kind {
            GossipKind::ChannelUpdate => chan_upd += 1,
            GossipKind::NodeAnnouncement => node_ann += 1,
            GossipKind::ChannelAnnouncement => chan_ann += 1,
        }
    }
    let secs = window.as_secs_f64().max(1e-9);
    let total = chan_upd + node_ann + chan_ann;
    let pct = |x: u64| -> f64 { 100.0 * x as f64 / total as f64 };
    println!(
        "  by kind: chan_update={} ({:.1}%, {:.2}/s), \
                  node_ann={} ({:.1}%, {:.2}/s), \
                  chan_ann={} ({:.1}%, {:.2}/s)",
        chan_upd, pct(chan_upd), chan_upd as f64 / secs,
        node_ann, pct(node_ann), node_ann as f64 / secs,
        chan_ann, pct(chan_ann), chan_ann as f64 / secs,
    );
}


/// Return the sketch protocol's `stagger` in seconds if the run uses
/// sketch — either as a homogeneous algo or in a Mix population. For
/// Mix runs the homogeneous-algo formula is a rough first-order fit;
/// it's exact when the whole population is sketch. None for non-sketch
/// runs (the predictor doesn't model flooding / cln / lnd dynamics).
fn sketch_stagger_secs(algo: &AlgoCfg) -> Option<f64> {
    match algo {
        AlgoCfg::Sketch { stagger_ms, .. } => Some(*stagger_ms as f64 / 1000.0),
        AlgoCfg::Mix { population } => population.iter().find_map(|e| {
            if let NodeAlgoKind::Sketch { stagger_ms, .. } = &e.algo {
                Some(*stagger_ms as f64 / 1000.0)
            } else {
                None
            }
        }),
        _ => None,
    }
}

/// Pre-schedule every origination event into the simulator's priority
/// queue before stepping starts. This replaces the previous per-event
/// `process_event` injection that ran inside the driver loop and
/// turned each event into an executor-wide synchronisation barrier.
/// With pre-scheduling the executor processes the entire stream
/// inline during `step_until`, with no driver round-trip per event.
///
/// `Scheduler::schedule_event` requires deadlines strictly in the
/// future of the current sim time. At pre-launch the current time
/// is `EPOCH`, so `delay = 0` events get bumped by 1 ns — sub-ms
/// latency model means observable timing is unchanged.
fn prelaunch_events(
    simu: &Simulation,
    originate_sources: &HashMap<NodeId, EventId<Gossip>>,
    events: Vec<(Duration, NodeId, Gossip)>,
) -> Result<()> {
    let scheduler = simu.scheduler();
    let t0 = MonotonicTime::EPOCH;
    for (delay, src, msg) in events {
        let event_id = originate_sources
            .get(&src)
            .ok_or_else(|| anyhow::anyhow!("no originate source for node id {src}"))?;
        let deadline = if delay.is_zero() {
            t0 + Duration::from_nanos(1)
        } else {
            t0 + delay
        };
        scheduler
            .schedule_event(deadline, event_id, msg)
            .map_err(|e| anyhow::anyhow!("schedule_event failed: {e:?}"))?;
    }
    Ok(())
}

/// Step the simulation forward to `deadline`, emitting a progress line
/// every `progress_interval` of simulated time. With pre-scheduled
/// origination events (see [`prelaunch_events`]) the driver only has
/// to advance time; the executor pops everything else from its
/// priority queue inline.
fn drive_simulation(
    simu: &mut Simulation,
    metrics: &MetricsHandle,
    deadline: MonotonicTime,
    progress_interval_secs: u64,
) -> Result<()> {
    let wall_start = Instant::now();
    let progress_interval = if progress_interval_secs > 0 {
        Some(Duration::from_secs(progress_interval_secs))
    } else {
        None
    };
    let mut next_progress = progress_interval.map(|p| MonotonicTime::EPOCH + p);
    let mut last_progress_events = 0usize;
    let mut last_progress_wall = wall_start;

    loop {
        if simu.time() >= deadline {
            break;
        }

        let target = match next_progress {
            Some(np) if np < deadline => np,
            _ => deadline,
        };

        if target > simu.time() {
            simu.step_until(target)
                .map_err(|e| anyhow::anyhow!("step_until failed: {e:?}"))?;
        }

        if let Some(np) = next_progress
            && simu.time() >= np
        {
            let now_wall = Instant::now();
            let total_events = metrics.total_first_seen();
            let delta_events = total_events - last_progress_events;
            let delta_wall = now_wall.duration_since(last_progress_wall).as_secs_f64();
            let rate = if delta_wall > 0.0 {
                delta_events as f64 / delta_wall
            } else {
                0.0
            };
            let sim_secs = simu
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();
            let wall_secs = now_wall.duration_since(wall_start).as_secs_f64();
            println!(
                "[t={:>8.1}s | wall={:>6.1}s] first_seen={:>10} (+{:>9}, {:>9.0}/wall_s)",
                sim_secs, wall_secs, total_events, delta_events, rate
            );
            last_progress_events = total_events;
            last_progress_wall = now_wall;
            next_progress = progress_interval.map(|p| np + p);
        }
    }

    Ok(())
}

/// Build a simulation where every node runs the flooding algorithm.
///
/// Setup is the canonical NeXosim shape:
/// 1. Allocate one model per node and one mailbox per node.
/// 2. For each directed edge (src, dst) in the topology, call
///    `nodes[src].out.connect(FloodingNode::recv, &mboxes[dst])`. After
///    this, `nodes[src].out.send(...)` will fan out to every wired peer's
///    `recv` input.
/// 3. Register one `EventSource` per node bound to that node's
///    `originate` method. The returned `EventId` is what we hand to
///    `process_event` later to inject a fresh message at runtime.
/// 4. Move models + mailboxes into the `SimInit`, call `init`, hand the
///    resulting `Simulation` to the chunked driver.
fn run_flooding(
    cfg: &SimConfig,
    topology: &Topology,
    metrics: MetricsHandle,
    forward_delay: Duration,
    run_duration: Duration,
    events: Vec<(Duration, NodeId, Gossip)>,
    deadline: MonotonicTime,
) -> Result<()> {
    let n = topology.len();
    // One Arc<NodeState> per node, shared with the model that owns
    // it AND with each of that node's peers (for sketch-style diffs).
    // The local `registry` is dropped at the end of this function;
    // the per-node Arcs survive via the model + its peers' clones.
    let registry = state::build_registry(n);
    // Drive construction by NodeIndex so `idx` is dense 0..n while
    // `id` is whatever NodeMeta carries (dense for synthetic, sparse
    // u64 hash for CSV-loaded).
    let mut nodes: Vec<FloodingNode> = topology
        .peers
        .node_indices()
        .map(|nx| {
            let meta = &topology.peers[nx];
            let state = registry[meta.idx as usize].clone();
            let peer_states: Vec<SharedNodeState> = topology
                .peers
                .neighbors(nx)
                .map(|ny| registry[topology.peers[ny].idx as usize].clone())
                .collect();
            FloodingNode::new(
                meta.id,
                meta.idx,
                forward_delay,
                run_duration,
                state,
                peer_states,
                metrics.clone(),
            )
        })
        .collect();
    let mboxes: Vec<Mailbox<FloodingNode>> = (0..n)
        .map(|_| Mailbox::with_capacity(cfg.run.mailbox_capacity))
        .collect();

    // Per-peer outputs: one Output per directed edge. The neighbour
    // iteration order matches the `peer_states` order above so
    // `outputs[i]` <-> `peer_states[i]` line up.
    for nx in topology.peers.node_indices() {
        for ny in topology.peers.neighbors(nx) {
            let mut out = Output::default();
            out.connect(FloodingNode::recv, &mboxes[ny.index()]);
            let peer_id = topology.peers[ny].id;
            nodes[nx.index()].add_peer(peer_id, out);
        }
    }

    let mut bench = new_sim_init(cfg);
    let mut originate_sources: HashMap<NodeId, EventId<Gossip>> = HashMap::with_capacity(n);
    for nx in topology.peers.node_indices() {
        let id = topology.peers[nx].id;
        let event_id = EventSource::<Gossip>::new()
            .connect(FloodingNode::originate, &mboxes[nx.index()])
            .register(&mut bench);
        originate_sources.insert(id, event_id);
    }

    for (i, (node, mbox)) in nodes.into_iter().zip(mboxes).enumerate() {
        let name = format!("n{i}");
        bench = bench.add_model(node, mbox, &name);
    }

    let mut simu = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|e| anyhow::anyhow!("simulation init failed: {e:?}"))?;

    prelaunch_events(&simu, &originate_sources, events)?;
    drive_simulation(
        &mut simu,
        &metrics,
        deadline,
        cfg.run.progress_interval_seconds,
    )
}

/// Build a simulation where every node runs a stagger algorithm — Cln,
/// Lnd, or any mix of the two read from per-vertex `NodeMeta::algo` on
/// the topology graph.
///
/// Heterogeneous populations need a bit more bookkeeping than the
/// flooding case because each kind has its own concrete model type, so
/// `Vec<ClnNode>` and `Vec<LndNode>` can't live in the same vector. We
/// keep them in separate vectors, then maintain `cln_local[i]` /
/// `lnd_local[i]` lookup tables that map a *global* node ID `i` to the
/// node's index in whichever per-kind vector owns it. Wiring then
/// dispatches on the (src_kind, dst_kind) pair.
///
/// Cross-kind connections work transparently because both `ClnNode::recv`
/// and `LndNode::recv` accept the same `WireMessage` type — that's the
/// whole reason `WireMessage` exists.
fn run_stagger_population(
    cfg: &SimConfig,
    topology: &Topology,
    metrics: MetricsHandle,
    run_duration: Duration,
    events: Vec<(Duration, NodeId, Gossip)>,
    deadline: MonotonicTime,
) -> Result<()> {
    let n = topology.len();

    let registry = state::build_registry(n);

    // {cln,lnd,sketch}_local[i] maps a vertex's NodeIndex (dense
    // 0..n) to its position in the per-type Vec<...>. Per-kind
    // vectors are separate because the model types differ.
    let mut cln_local: Vec<Option<usize>> = vec![None; n];
    let mut lnd_local: Vec<Option<usize>> = vec![None; n];
    let mut sketch_local: Vec<Option<usize>> = vec![None; n];
    let mut cln_nodes: Vec<ClnNode> = Vec::new();
    let mut lnd_nodes: Vec<LndNode> = Vec::new();
    let mut sketch_nodes: Vec<SketchNode> = Vec::new();
    let mut cln_mboxes: Vec<Mailbox<ClnNode>> = Vec::new();
    let mut lnd_mboxes: Vec<Mailbox<LndNode>> = Vec::new();
    let mut sketch_mboxes: Vec<Mailbox<SketchNode>> = Vec::new();

    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed ^ 0xC1A);

    for nx in topology.peers.node_indices() {
        let meta = &topology.peers[nx];
        let id = meta.id;
        let idx = meta.idx;
        let algo = &meta.algo;
        let phase = sample_phase(&mut rng, stagger_of(algo));
        let state = registry[idx as usize].clone();
        let peer_states: Vec<SharedNodeState> = topology
            .peers
            .neighbors(nx)
            .map(|ny| registry[topology.peers[ny].idx as usize].clone())
            .collect();
        match algo {
            NodeAlgo::Cln { stagger_ms } => {
                cln_local[nx.index()] = Some(cln_nodes.len());
                cln_nodes.push(ClnNode::new(
                    id,
                    idx,
                    Duration::from_millis(*stagger_ms),
                    phase,
                    run_duration,
                    state,
                    peer_states,
                    metrics.clone(),
                ));
                cln_mboxes.push(Mailbox::with_capacity(cfg.run.mailbox_capacity));
            }
            NodeAlgo::Lnd {
                stagger_ms,
                trickle_ms,
                min_batch_size,
            } => {
                lnd_local[nx.index()] = Some(lnd_nodes.len());
                lnd_nodes.push(LndNode::new(
                    id,
                    idx,
                    Duration::from_millis(*stagger_ms),
                    phase,
                    Duration::from_millis(*trickle_ms),
                    *min_batch_size,
                    run_duration,
                    state,
                    peer_states,
                    metrics.clone(),
                ));
                lnd_mboxes.push(Mailbox::with_capacity(cfg.run.mailbox_capacity));
            }
            NodeAlgo::Sketch {
                stagger_ms,
                capacity_chan_updates,
                capacity_node_anns,
                capacity_chan_anns,
            } => {
                sketch_local[nx.index()] = Some(sketch_nodes.len());
                sketch_nodes.push(SketchNode::new(
                    id,
                    idx,
                    Duration::from_millis(*stagger_ms),
                    *capacity_chan_updates,
                    *capacity_node_anns,
                    *capacity_chan_anns,
                    run_duration,
                    state,
                    peer_states,
                    metrics.clone(),
                ));
                sketch_mboxes.push(Mailbox::with_capacity(cfg.run.mailbox_capacity));
            }
            NodeAlgo::Flooding => {
                panic!(
                    "run_stagger_population received a Flooding node \
                     (id={id}); flooding populations should go through \
                     run_flooding"
                );
            }
        }
    }

    // Wire connections by (src_kind, dst_kind) over the peer graph.
    // Each (src, dst) edge gets a fresh per-peer Output on the source
    // node, connected to the destination's mailbox.
    for nx in topology.peers.node_indices() {
        for ny in topology.peers.neighbors(nx) {
            wire_connection(
                nx.index(),
                ny.index(),
                topology,
                &mut cln_nodes,
                &mut lnd_nodes,
                &mut sketch_nodes,
                &cln_mboxes,
                &lnd_mboxes,
                &sketch_mboxes,
                &cln_local,
                &lnd_local,
                &sketch_local,
            );
        }
    }

    // Sample per-peer sketch offsets — one per (sketch_node, peer) pair.
    // Deterministic from cfg.seed XOR the node's id so reruns with the
    // same config produce identical schedules.
    for sn in sketch_nodes.iter_mut() {
        let stagger = sn.stagger_for_offset_sample();
        let mut sn_rng = ChaCha8Rng::seed_from_u64(cfg.seed ^ 0x5C7E ^ sn.id);
        let n_peers = sn.outputs.len();
        let mut offsets = Vec::with_capacity(n_peers);
        for _ in 0..n_peers {
            offsets.push(sample_phase(&mut sn_rng, stagger));
        }
        sn.set_per_peer_offsets(offsets);
    }

    let mut bench = new_sim_init(cfg);

    // Build originate sources keyed by NodeId. The driver's
    // `process_event` looks them up by id; for CSV-loaded sparse ids
    // a HashMap is necessary, and it works for synthetic too.
    let mut originate_sources: HashMap<NodeId, EventId<Gossip>> = HashMap::with_capacity(n);
    for nx in topology.peers.node_indices() {
        let meta = &topology.peers[nx];
        let event_id = match &meta.algo {
            NodeAlgo::Cln { .. } => EventSource::<Gossip>::new()
                .connect(
                    ClnNode::originate,
                    &cln_mboxes[cln_local[nx.index()].unwrap()],
                )
                .register(&mut bench),
            NodeAlgo::Lnd { .. } => EventSource::<Gossip>::new()
                .connect(
                    LndNode::originate,
                    &lnd_mboxes[lnd_local[nx.index()].unwrap()],
                )
                .register(&mut bench),
            NodeAlgo::Sketch { .. } => EventSource::<Gossip>::new()
                .connect(
                    SketchNode::originate,
                    &sketch_mboxes[sketch_local[nx.index()].unwrap()],
                )
                .register(&mut bench),
            NodeAlgo::Flooding => unreachable!("guarded above"),
        };
        originate_sources.insert(meta.id, event_id);
    }

    // Add models.
    for (local_idx, (node, mbox)) in cln_nodes.into_iter().zip(cln_mboxes).enumerate() {
        bench = bench.add_model(node, mbox, &format!("cln{local_idx}"));
    }
    for (local_idx, (node, mbox)) in lnd_nodes.into_iter().zip(lnd_mboxes).enumerate() {
        bench = bench.add_model(node, mbox, &format!("lnd{local_idx}"));
    }
    for (local_idx, (node, mbox)) in sketch_nodes.into_iter().zip(sketch_mboxes).enumerate() {
        bench = bench.add_model(node, mbox, &format!("sketch{local_idx}"));
    }

    let mut simu = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|e| anyhow::anyhow!("simulation init failed: {e:?}"))?;

    prelaunch_events(&simu, &originate_sources, events)?;
    drive_simulation(
        &mut simu,
        &metrics,
        deadline,
        cfg.run.progress_interval_seconds,
    )
}

fn stagger_of(algo: &NodeAlgo) -> Duration {
    match algo {
        NodeAlgo::Cln { stagger_ms } => Duration::from_millis(*stagger_ms),
        NodeAlgo::Lnd { stagger_ms, .. } => Duration::from_millis(*stagger_ms),
        NodeAlgo::Sketch { stagger_ms, .. } => Duration::from_millis(*stagger_ms),
        NodeAlgo::Flooding => Duration::ZERO,
    }
}

#[allow(clippy::too_many_arguments)]
fn wire_connection(
    src: usize,
    dst: usize,
    topology: &Topology,
    cln_nodes: &mut [ClnNode],
    lnd_nodes: &mut [LndNode],
    sketch_nodes: &mut [SketchNode],
    cln_mboxes: &[Mailbox<ClnNode>],
    lnd_mboxes: &[Mailbox<LndNode>],
    sketch_mboxes: &[Mailbox<SketchNode>],
    cln_local: &[Option<usize>],
    lnd_local: &[Option<usize>],
    sketch_local: &[Option<usize>],
) {
    let src_meta = &topology.peers[petgraph::graph::NodeIndex::new(src)];
    let dst_meta = &topology.peers[petgraph::graph::NodeIndex::new(dst)];
    let dst_id = dst_meta.id;
    match (&src_meta.algo, &dst_meta.algo) {
        // Cln source
        (NodeAlgo::Cln { .. }, NodeAlgo::Cln { .. }) => {
            let s = cln_local[src].unwrap();
            let d = cln_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(ClnNode::recv, &cln_mboxes[d]);
            cln_nodes[s].add_peer(dst_id, out);
        }
        (NodeAlgo::Cln { .. }, NodeAlgo::Lnd { .. }) => {
            let s = cln_local[src].unwrap();
            let d = lnd_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(LndNode::recv, &lnd_mboxes[d]);
            cln_nodes[s].add_peer(dst_id, out);
        }
        (NodeAlgo::Cln { .. }, NodeAlgo::Sketch { .. }) => {
            let s = cln_local[src].unwrap();
            let d = sketch_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(SketchNode::recv, &sketch_mboxes[d]);
            cln_nodes[s].add_peer(dst_id, out);
        }
        // Lnd source
        (NodeAlgo::Lnd { .. }, NodeAlgo::Cln { .. }) => {
            let s = lnd_local[src].unwrap();
            let d = cln_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(ClnNode::recv, &cln_mboxes[d]);
            lnd_nodes[s].add_peer(dst_id, out);
        }
        (NodeAlgo::Lnd { .. }, NodeAlgo::Lnd { .. }) => {
            let s = lnd_local[src].unwrap();
            let d = lnd_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(LndNode::recv, &lnd_mboxes[d]);
            lnd_nodes[s].add_peer(dst_id, out);
        }
        (NodeAlgo::Lnd { .. }, NodeAlgo::Sketch { .. }) => {
            let s = lnd_local[src].unwrap();
            let d = sketch_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(SketchNode::recv, &sketch_mboxes[d]);
            lnd_nodes[s].add_peer(dst_id, out);
        }
        // Sketch source
        (NodeAlgo::Sketch { .. }, NodeAlgo::Cln { .. }) => {
            let s = sketch_local[src].unwrap();
            let d = cln_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(ClnNode::recv, &cln_mboxes[d]);
            sketch_nodes[s].add_peer(dst_id, out);
        }
        (NodeAlgo::Sketch { .. }, NodeAlgo::Lnd { .. }) => {
            let s = sketch_local[src].unwrap();
            let d = lnd_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(LndNode::recv, &lnd_mboxes[d]);
            sketch_nodes[s].add_peer(dst_id, out);
        }
        (NodeAlgo::Sketch { .. }, NodeAlgo::Sketch { .. }) => {
            let s = sketch_local[src].unwrap();
            let d = sketch_local[dst].unwrap();
            let mut out = Output::default();
            out.connect(SketchNode::recv, &sketch_mboxes[d]);
            sketch_nodes[s].add_peer(dst_id, out);
        }
        _ => unreachable!("Flooding nodes are routed via run_flooding"),
    }
}

/// Sample a tick phase uniformly in (0, stagger].
///
/// Strictly positive so the very first tick is always *after* `t=0`,
/// which means a node's own originated message at `t=0` is guaranteed to
/// be in the pending queue when its first tick fires.
///
/// Random per-node phasing also avoids the "every node ticks at the same
/// absolute time" pathology that lets a single message cascade many hops
/// in one time step (the tick-vs-recv ordering in NeXosim isn't
/// guaranteed at coincident times, so synchronous phases produce wildly
/// faster propagation than the stagger algorithm should permit).
fn sample_phase(rng: &mut ChaCha8Rng, stagger: Duration) -> Duration {
    use statrs::distribution::{ContinuousCDF, Uniform};
    let stagger_secs = stagger.as_secs_f64();
    let dist = Uniform::new(0.0, stagger_secs).expect("stagger must be positive");
    // statrs samples via inverse-CDF — see the note in events/poisson.rs
    // for why we don't call its `sample` directly. Floor at 1 ns so the
    // first tick is strictly later than t=0.
    let u = rng.random::<f64>().clamp(f64::EPSILON, 1.0 - f64::EPSILON);
    let secs = dist.inverse_cdf(u).max(1e-9);
    Duration::from_secs_f64(secs)
}

/// Short identifier for the topology kind, used in the auto-generated
/// stats output filename.
fn topology_kind_name(c: &TopologyCfg) -> &'static str {
    match c {
        TopologyCfg::KRegular { .. } => "k_regular",
        TopologyCfg::FromCsv { .. } => "from_csv",
    }
}

/// Short identifier for the algorithm kind, used in the auto-generated
/// stats output filename.
fn algo_kind_name(c: &AlgoCfg) -> &'static str {
    match c {
        AlgoCfg::Flooding {} => "flooding",
        AlgoCfg::Cln { .. } => "cln",
        AlgoCfg::Lnd { .. } => "lnd",
        AlgoCfg::Mix { .. } => "mix",
        AlgoCfg::Sketch { .. } => "sketch",
    }
}

/// Short identifier for the event source kind, used in the
/// auto-generated stats output filename.
fn event_kind_name(c: &EventCfg) -> &'static str {
    match c {
        EventCfg::OneShotSingle { .. } => "oneshot_single",
        EventCfg::OneShotAll {} => "oneshot_all",
        EventCfg::PoissonRandom { .. } => "poisson",
        EventCfg::ParquetReplay { .. } => "parquet_replay",
    }
}
