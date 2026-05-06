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

use std::time::{Duration, Instant};

use anyhow::Result;
use nexosim::ports::EventSource;
use nexosim::simulation::{EventId, Mailbox, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::config::{AlgoCfg, EventCfg, LatencyCfg, NodeAlgoKind, SimConfig, TopologyCfg};
use crate::events::EventSchedule;
use crate::events::oneshot::{OneShotAll, OneShotSingle};
use crate::events::poisson::PoissonRandom;
use crate::message::Gossip;
use crate::metrics::MetricsHandle;
use crate::node::cln::ClnNode;
use crate::node::flooding::FloodingNode;
use crate::node::lnd::LndNode;
use crate::topology::{Topology, metrics as topology_metrics, synthetic};

pub struct RunResult {
    pub metrics: MetricsHandle,
    pub topology: Topology,
}

/// Top-level entry point: build the topology, choose the right node
/// builder for the configured algorithm, kick off the chunked driver,
/// return the (still-locked) metrics for post-run reporting.
pub fn run(cfg: &SimConfig) -> Result<RunResult> {
    let topology = match &cfg.topology {
        TopologyCfg::KRegular { n, k } => synthetic::random_regular(*n, *k, cfg.seed),
    };
    let topo_stats = topology_metrics::compute(&topology, cfg.seed, 2000, 1000);
    println!("topology: {topo_stats:#?}");
    let metrics = MetricsHandle::default();
    let n = topology.len();
    let run_duration = Duration::from_secs(cfg.run.duration_seconds);

    let event_tuples = build_events(cfg, n, run_duration);
    println!(
        "events: scheduled {} message(s) over the run window",
        event_tuples.len()
    );
    let deadline = MonotonicTime::EPOCH + run_duration;

    match &cfg.algo {
        AlgoCfg::Flooding {} => run_flooding(
            cfg,
            &topology,
            metrics.clone(),
            Duration::from_millis(latency_ms(&cfg.latency)),
            event_tuples,
            deadline,
        )?,
        AlgoCfg::Cln { stagger_ms } => {
            let assignments = vec![NodeAlgoKind::Cln { stagger_ms: *stagger_ms }; n];
            run_stagger_population(cfg, &topology, metrics.clone(), assignments, event_tuples, deadline)?;
        }
        AlgoCfg::Lnd {
            stagger_ms,
            trickle_ms,
            min_batch_size,
        } => {
            let assignments = vec![
                NodeAlgoKind::Lnd {
                    stagger_ms: *stagger_ms,
                    trickle_ms: *trickle_ms,
                    min_batch_size: *min_batch_size,
                };
                n
            ];
            run_stagger_population(cfg, &topology, metrics.clone(), assignments, event_tuples, deadline)?;
        }
        AlgoCfg::Mix { population } => {
            let assignments = assign_mix(n, population, cfg.seed);
            log_mix_summary(&assignments);
            run_stagger_population(cfg, &topology, metrics.clone(), assignments, event_tuples, deadline)?;
        }
    }

    Ok(RunResult { metrics, topology })
}

fn latency_ms(c: &LatencyCfg) -> u64 {
    match c {
        LatencyCfg::Constant { ms } => *ms,
    }
}

fn build_events(cfg: &SimConfig, n: usize, max: Duration) -> Vec<(Duration, u32, Gossip)> {
    let mut tuples = match &cfg.event {
        EventCfg::OneShotSingle { node } => OneShotSingle {
            node: *node,
            at: Duration::ZERO,
            size_bytes: 1024,
        }
        .build(n, max),
        EventCfg::OneShotAll {} => OneShotAll {
            at: Duration::ZERO,
            size_bytes: 1024,
        }
        .build(n, max),
        EventCfg::PoissonRandom {
            rate_per_sec,
            size_bytes,
        } => PoissonRandom {
            rate_per_sec: *rate_per_sec,
            seed: cfg.seed ^ 0xE7E,
            size_bytes: *size_bytes,
        }
        .build(n, max),
    };
    tuples.sort_by_key(|(t, _, _)| *t);
    tuples
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

/// Step the simulation in chunks, injecting scheduled events as their times
/// arrive and printing a progress line every `progress_interval` of sim time.
///
/// The loop's job each iteration is to figure out the *closest* future
/// thing we care about — the deadline, the next scheduled origination, or
/// the next progress checkpoint — step time to it, then handle whatever
/// got triggered. Events are kept in a sorted iterator (peek-and-take
/// pattern) so we never re-scan them.
fn drive_simulation(
    simu: &mut Simulation,
    originate_sources: Vec<EventId<Gossip>>,
    events: Vec<(Duration, u32, Gossip)>,
    metrics: &MetricsHandle,
    deadline: MonotonicTime,
    progress_interval_secs: u64,
) -> Result<()> {
    let mut events_iter = events.into_iter();
    let mut next_event = events_iter.next();
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

        // Find next checkpoint: deadline, next event, or next progress tick.
        let mut target = deadline;
        if let Some((t, _, _)) = &next_event {
            target = target.min(MonotonicTime::EPOCH + *t);
        }
        if let Some(np) = next_progress {
            target = target.min(np);
        }

        if target > simu.time() {
            simu.step_until(target)
                .map_err(|e| anyhow::anyhow!("step_until failed: {e:?}"))?;
        }

        // Drain any events that are due now (after the step).
        while let Some(peek) = next_event.as_ref() {
            let event_time = MonotonicTime::EPOCH + peek.0;
            if event_time <= simu.time() {
                let (_t, src, msg) = next_event.take().unwrap();
                simu.process_event(&originate_sources[src as usize], msg)
                    .map_err(|e| anyhow::anyhow!("process_event failed: {e:?}"))?;
                next_event = events_iter.next();
            } else {
                break;
            }
        }

        // Maybe emit progress.
        if let Some(np) = next_progress
            && simu.time() >= np {
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
    events: Vec<(Duration, u32, Gossip)>,
    deadline: MonotonicTime,
) -> Result<()> {
    let n = topology.len();
    let mut nodes: Vec<FloodingNode> = (0..n)
        .map(|i| FloodingNode::new(i as u32, forward_delay, metrics.clone()))
        .collect();
    let mboxes: Vec<Mailbox<FloodingNode>> = (0..n)
        .map(|_| Mailbox::with_capacity(cfg.run.mailbox_capacity))
        .collect();

    for (i, peers) in topology.iter().enumerate() {
        for &j in peers {
            nodes[i].out.connect(FloodingNode::recv, &mboxes[j as usize]);
        }
    }

    let mut bench = SimInit::new();
    let originate_sources: Vec<EventId<Gossip>> = (0..n)
        .map(|i| {
            EventSource::<Gossip>::new()
                .connect(FloodingNode::originate, &mboxes[i])
                .register(&mut bench)
        })
        .collect();

    for (i, (node, mbox)) in nodes.into_iter().zip(mboxes.into_iter()).enumerate() {
        let name = format!("n{i}");
        bench = bench.add_model(node, mbox, &name);
    }

    let mut simu = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|e| anyhow::anyhow!("simulation init failed: {e:?}"))?;

    drive_simulation(
        &mut simu,
        originate_sources,
        events,
        &metrics,
        deadline,
        cfg.run.progress_interval_seconds,
    )
}

/// Build a simulation where every node runs a stagger algorithm — Cln,
/// Lnd, or any mix of the two given by `assignments[i]`.
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
    assignments: Vec<NodeAlgoKind>,
    events: Vec<(Duration, u32, Gossip)>,
    deadline: MonotonicTime,
) -> Result<()> {
    let n = topology.len();
    assert_eq!(assignments.len(), n);

    let mut cln_local: Vec<Option<usize>> = vec![None; n];
    let mut lnd_local: Vec<Option<usize>> = vec![None; n];
    let mut cln_nodes: Vec<ClnNode> = Vec::new();
    let mut lnd_nodes: Vec<LndNode> = Vec::new();
    let mut cln_mboxes: Vec<Mailbox<ClnNode>> = Vec::new();
    let mut lnd_mboxes: Vec<Mailbox<LndNode>> = Vec::new();

    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed ^ 0xC1A);

    for (i, algo) in assignments.iter().enumerate() {
        let phase = sample_phase(&mut rng, stagger_of(algo));
        match algo {
            NodeAlgoKind::Cln { stagger_ms } => {
                cln_local[i] = Some(cln_nodes.len());
                cln_nodes.push(ClnNode::new(
                    i as u32,
                    Duration::from_millis(*stagger_ms),
                    phase,
                    metrics.clone(),
                ));
                cln_mboxes.push(Mailbox::with_capacity(cfg.run.mailbox_capacity));
            }
            NodeAlgoKind::Lnd {
                stagger_ms,
                trickle_ms,
                min_batch_size,
            } => {
                lnd_local[i] = Some(lnd_nodes.len());
                lnd_nodes.push(LndNode::new(
                    i as u32,
                    Duration::from_millis(*stagger_ms),
                    phase,
                    Duration::from_millis(*trickle_ms),
                    *min_batch_size,
                    metrics.clone(),
                ));
                lnd_mboxes.push(Mailbox::with_capacity(cfg.run.mailbox_capacity));
            }
        }
    }

    // Wire connections by (src_kind, dst_kind).
    for (src, peers) in topology.iter().enumerate() {
        for &dst in peers {
            let dst = dst as usize;
            wire_connection(
                src,
                dst,
                &assignments,
                &mut cln_nodes,
                &mut lnd_nodes,
                &cln_mboxes,
                &lnd_mboxes,
                &cln_local,
                &lnd_local,
            );
        }
    }

    let mut bench = SimInit::new();

    // Build originate sources before consuming nodes/mboxes.
    let mut originate_sources: Vec<Option<EventId<Gossip>>> = Vec::with_capacity(n);
    for (i, algo) in assignments.iter().enumerate() {
        let src = match algo {
            NodeAlgoKind::Cln { .. } => EventSource::<Gossip>::new()
                .connect(ClnNode::originate, &cln_mboxes[cln_local[i].unwrap()])
                .register(&mut bench),
            NodeAlgoKind::Lnd { .. } => EventSource::<Gossip>::new()
                .connect(LndNode::originate, &lnd_mboxes[lnd_local[i].unwrap()])
                .register(&mut bench),
        };
        originate_sources.push(Some(src));
    }
    let originate_sources: Vec<EventId<Gossip>> =
        originate_sources.into_iter().map(|s| s.unwrap()).collect();

    // Add models.
    for (local_idx, (node, mbox)) in cln_nodes.into_iter().zip(cln_mboxes.into_iter()).enumerate() {
        bench = bench.add_model(node, mbox, &format!("cln{local_idx}"));
    }
    for (local_idx, (node, mbox)) in lnd_nodes.into_iter().zip(lnd_mboxes.into_iter()).enumerate() {
        bench = bench.add_model(node, mbox, &format!("lnd{local_idx}"));
    }

    let mut simu = bench
        .init(MonotonicTime::EPOCH)
        .map_err(|e| anyhow::anyhow!("simulation init failed: {e:?}"))?;

    drive_simulation(
        &mut simu,
        originate_sources,
        events,
        &metrics,
        deadline,
        cfg.run.progress_interval_seconds,
    )
}

fn stagger_of(algo: &NodeAlgoKind) -> Duration {
    match algo {
        NodeAlgoKind::Cln { stagger_ms } => Duration::from_millis(*stagger_ms),
        NodeAlgoKind::Lnd { stagger_ms, .. } => Duration::from_millis(*stagger_ms),
    }
}

fn wire_connection(
    src: usize,
    dst: usize,
    assignments: &[NodeAlgoKind],
    cln_nodes: &mut [ClnNode],
    lnd_nodes: &mut [LndNode],
    cln_mboxes: &[Mailbox<ClnNode>],
    lnd_mboxes: &[Mailbox<LndNode>],
    cln_local: &[Option<usize>],
    lnd_local: &[Option<usize>],
) {
    let src_algo = &assignments[src];
    let dst_algo = &assignments[dst];
    match (src_algo, dst_algo) {
        (NodeAlgoKind::Cln { .. }, NodeAlgoKind::Cln { .. }) => {
            let s = cln_local[src].unwrap();
            let d = cln_local[dst].unwrap();
            cln_nodes[s].out.connect(ClnNode::recv, &cln_mboxes[d]);
        }
        (NodeAlgoKind::Cln { .. }, NodeAlgoKind::Lnd { .. }) => {
            let s = cln_local[src].unwrap();
            let d = lnd_local[dst].unwrap();
            cln_nodes[s].out.connect(LndNode::recv, &lnd_mboxes[d]);
        }
        (NodeAlgoKind::Lnd { .. }, NodeAlgoKind::Cln { .. }) => {
            let s = lnd_local[src].unwrap();
            let d = cln_local[dst].unwrap();
            lnd_nodes[s].out.connect(ClnNode::recv, &cln_mboxes[d]);
        }
        (NodeAlgoKind::Lnd { .. }, NodeAlgoKind::Lnd { .. }) => {
            let s = lnd_local[src].unwrap();
            let d = lnd_local[dst].unwrap();
            lnd_nodes[s].out.connect(LndNode::recv, &lnd_mboxes[d]);
        }
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
    let max_ns = stagger.as_nanos() as u64;
    let ns = rng.random_range(1..=max_ns.max(1));
    Duration::from_nanos(ns)
}
