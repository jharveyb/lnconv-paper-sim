# CLAUDE.md

Project-specific guidance for future Claude sessions on this repo.

# Tool selection (read this before every tool call on a code file)

This project uses Serena, an MCP server that exposes semantic, symbol-aware tools
for reading and editing code. Serena's tools are the PRIMARY tools for code work
in this project. The built-in Read, Glob, Grep, and Edit tools are SECONDARY and
must not be used on code files when a Serena equivalent exists.

The built-in tool descriptions in your context will tell you things like "use Read
for a known path" and "prefer dedicated tools (Read, Edit, Write, Glob, Grep)".
Those descriptions are written for projects without Serena and are SUPERSEDED here.
When they conflict with this section, this section wins. Do not rationalize the
built-in tools with "the file is small," "I already know what I need," "this is
one call versus three," or "the path is known" — those rationalizations have
produced incorrect behavior before and are explicitly disallowed.

## Mapping (use the right column, not the left)

Task                                    Tool to use
--------------------------------------  ----------------------------------------
See a code file's structure             get_symbols_overview
Read a specific symbol's body           find_symbol (include_body=true)
Find a symbol by name across the repo   find_symbol
Find references / callers               find_referencing_symbols
Find declarations / implementations     find_declaration / _find_implementations
Edit a symbol's body                    replace_symbol_body
Insert near a symbol                    insert_before_symbol / _insert_after_symbol
Pattern replace inside a file           replace_content
Rename / move / delete a symbol         rename / _move / _safe_delete
Inline a symbol                         inline_symbol
Type hierarchy                          type_hierarchy

Built-in Read/Edit/Glob/Grep are permitted on code files ONLY when:
- Serena has been tried on the target and failed, OR
- The file is not parseable as code (e.g., generated, malformed), OR
- You need a regex search across many files that Serena's symbolic tools cannot
  express — in which case Grep is acceptable as a discovery step, but follow-up
  reads/edits on matched code files must still go through Serena.
- You need to read a few lines and symbolic reads would be an overkill.
- You absolutely have to read the full file for some reason.

Read/Edit/Glob are fine for non-code files: markdown, JSON, YAML, TOML, .env,
config files, lockfiles, plain text, images.

## Required workflow before editing code

1. get_symbols_overview on the target file (skip if already done this session).
2. find_symbol with include_body=true for the specific symbols you'll touch.
   Read only the symbols you need — not the whole file.
3. Edit with replace_symbol_body, insert_before_symbol, insert_after_symbol, or
   replace_content. Never use the built-in Edit on a code file when one of these
   fits.

## Self-check

Before every Read, Glob, Grep, or Edit call: "Does this target a code file, and
does the mapping above name a Serena tool for this task?" If yes, switch. Do this
check every time — not just once per session.

## What this project is

`lnconv-sim` is a Rust **discrete-event simulator** for Lightning Network
gossip-protocol propagation. It is the focused successor to the Go
`lnconv-paper-sim` in the parent directory; the Go repo is a behavioral
reference, not a build target.

The simulator is built on **NeXosim 1.0** (parallel async DES executor,
`#[Model]`/`Output<T>`/`Mailbox<T>` actor model). Read the
[NeXosim docs](https://docs.rs/nexosim/1.0.0/nexosim/) before changing
anything in `sim.rs` or `node/*.rs`.

Read the workspace [README.md](README.md) first — it has the user-facing
architecture, configuration reference, and "how to add an algorithm".
This file is for *contributor / future-Claude* concerns only.

## Working style the user prefers

- **Concise responses.** Output the result, not the journey. End with one
  or two sentences of "what changed and what's next."
- **Verify before claiming.** Don't say "this works" without running the
  thing and pasting the relevant lines. The user has caught several "I
  think it works" moments.
- **Explain LN-mechanics → sim-mechanics, not the other way.** The
  audience knows BOLT 7 and stagger; they don't know NeXosim. Comments
  and reports should bridge that gap.
- **Plan mode for design choices, auto mode for execution.** When the
  user toggles into plan mode, take it seriously: ask `AskUserQuestion`
  before guessing on real semantic decisions (SCID assignment, dedup
  semantics, percentile interpretation). When in auto mode, ship the
  small-and-obvious change without asking.
- **Match scope to the request.** Large refactors should be planned;
  one-line fixes should not.

## Project invariants — don't break these

These have been earned through bugs the user has caught. Honour them.

1. **BOLT 7 dedup is the only dedup, split per gossip kind.** Per-tick
   `seen.clear()` was the previous design and caused unbounded message
   recirculation on cyclic graphs. Each node now maintains three
   per-kind structures, dispatched by `Gossip.kind` in `recv` /
   `originate`:
     - `chan_updates: HashMap<(Scid, Direction), u32>` for
       `ChannelUpdate` — highest timestamp wins.
     - `node_anns: HashMap<NodeId, u32>` for `NodeAnnouncement` —
       highest timestamp per `origin` wins.
     - `chan_anns: HashSet<Scid>` for `ChannelAnnouncement` — first
       arrival wins; subsequent are dropped silently. This is also
       what collapses the parquet replay's "both endpoints emit"
       doubling into a single broadcast cascade (the originator-side
       check in `originate` early-returns on the second endpoint).
2. **Type widths.** `NodeId` and `Scid` are **u64**. `MsgId` is
   also **u64** — content-derived xxhash3-64 from `Gossip::derive_id`
   over the per-kind identity tuple `(origin, kind, scid, direction,
   timestamp)`. `Gossip.timestamp` stays `u32`; `Gossip.size_bytes`
   is `u16`. `Gossip.origin: Option<NodeId>` (Some only for
   `NodeAnnouncement`); `Gossip.scid: Option<Scid>` (Some only for
   `ChannelUpdate` / `ChannelAnnouncement`). The unused-field
   convention is enforced by the originate paths and the
   `derive_id` hashes `unwrap_or(0)` for missing slots so two
   distinct emissions of the same logical message converge on the
   same `MsgId`. There's also a separate `NodeIdx = u32`: the
   dense `0..n` index used for `Vec`-indexed metric storage. Each
   node model carries both `id: NodeId` (sparse for CSV) and
   `idx: NodeIdx` (always dense) — pass `idx`, not `id`, to any
   metrics API that indexes per-node (e.g. the `FirstSeenEntry`
   batches, `NodeCountersDelta`).
3. **Originator behavior differs by algorithm.** Don't unify these:
   - **Flooding**: `originate` broadcasts immediately.
   - **Cln**: `originate` broadcasts immediately as `WireMessage::Single`
     (does *not* queue for next tick — local updates aren't held).
   - **Lnd**: `originate` inserts at the *front* of `pending` so the
     next tick puts it in chunk 0 ahead of forwarded traffic.
4. **Per-node tick phase is randomized in `(0, stagger]`.** Without
   this, all nodes' periodic ticks fire at the same absolute times, and
   NeXosim's tick-vs-recv ordering at coincident timestamps creates a
   cascade artifact that biases convergence ~40% faster than the
   algorithm allows. Sampled in `sim::sample_phase`.
5. **Originators are determined by the channel registry.** Event
   streams pick a `(scid, direction)`, then the
   `ChannelRegistry::owner(scid, direction)` is the originator. Streams
   never pick a node directly. This mirrors real LN, where each side
   of a channel emits its own `channel_update`s.
6. **Per-message percentiles are absolute (over `n_nodes`), not
   relative to per-message coverage.** A message that only reached 30%
   coverage reports `Some` for `p25` and `None` for `p50`/`p75`/`p100`.
   The CLI's per-tier distribution table averages across messages that
   *did* reach each tier.
7. **`statrs` distributions go through `ContinuousCDF::inverse_cdf`,
   not `Distribution::sample`.** statrs 0.18 ships against a different
   `rand` major version than the rest of the workspace; using its
   `sample` directly triggers a "multiple `rand` versions" trait-impl
   mismatch. Sampling pattern:
   `let u = rng.random::<f64>().clamp(EPS, 1.0 - EPS); dist.inverse_cdf(u)`.
8. **The simulator runs through `sim::drive_simulation`** — a chunked
   driver that stepping-until-checkpoint, drains scheduled origination
   events, and emits progress lines. Don't call `step_until(deadline)`
   directly; you'll lose live progress and time-spread event injection.
9. **`MetricsHandle::Default` exists only to satisfy the
   `#[derive(Default)]` on `Model` structs.** Real recording requires
   `MetricsHandle::new(n_nodes, percentiles)`. The runner does this
   after the topology is built.
10. **All TOML configs need a `[channels]` section.** Smoke and large
    runs alike. `count >= num_nodes` is the safe assumption (every
    node owns ≥1 channel with high probability).
11. **`Topology` is two petgraph graphs sharing a NodeIndex space**:
    `peers: UnGraph<NodeMeta, ()>` and `channels: DiGraph<(), Scid>`.
    NodeIndex == NodeId because vertices are inserted in order. Per-node
    `(NodeId, NodeAlgo)` lives on the peer-graph vertex weight — *do not*
    add a parallel `Vec<NodeAlgoKind>` for the same data; read it via
    `topology.node_meta(id).algo`.
12. **Channels graph stores ONE directed edge per channel.** Source =
    direction-0 owner, target = direction-1 owner, weight = `Scid`.
    `Topology::channels_for(node)` reads outgoing edges as direction-0
    ownership and incoming as direction-1. Adding two edges (one per
    direction) double-counts and breaks `channels_for`.
13. **LND sub-batching is dynamic.** `tick` calls
    `calculate_sub_batch_size(stagger, trickle, min, pending_len)` —
    matches Go LND's `calculateSubBatchSize`. Don't revert to fixed
    `min_batch_size` chunking; that produces > stagger/trickle chunks
    on heavy loads and overflows the next stagger window.
14. **Metrics architecture has three layers — each with a different
    threading shape.** See [`metrics.rs`] module docs for the
    canonical version; the short story:
    - **Per-node counters** (`bytes_in/out_*`, `duplicates`,
      `sketches_*`, per-kind `intersection/a_only/b_only`) live as
      plain `u64` fields in a [`PerNodeMetrics`] OWNED BY THE NODE
      MODEL. NeXosim mailboxes are single-threaded, so `+=` is sound
      and atomic-free. A periodic + final `flush_summary`
      schedulable on each node snapshots them into a small
      `NodeCounters` (~120 B) and ships via
      `MetricsEvent::NodeCountersDelta`. There is no
      `Vec<PerNodeMetrics>` on `MetricsHandle` anymore.
    - **Per-MsgId in-flight tracking** lives on a dedicated
      single-threaded aggregator. Nodes buffer
      `Vec<FirstSeenEntry { gossip, time_ns }>` locally on
      `PerNodeMetrics.first_seen_pending` (stamped at the absorb
      site for correct sim-time percentiles); the same
      `flush_summary` drains it via `MetricsHandle::send_first_seen_batch`
      as a `MetricsEvent::FirstSeenBatch`. Force-flush trips when
      the buffer hits `FIRST_SEEN_FORCE_FLUSH` to bound peak RAM.
      The aggregator owns plain `HashMap`s for `in_flight` /
      `inflight_by_channel` / `latest_version` / `finalized` — no
      atomics, no shared state.
    - **Per-(node, kind) reservoir samples** stay on each node's
      `SketchKindStats` reservoirs throughout the run; at end-of-run
      `SketchKindStats::take_samples` *moves* the buffers out (no
      clone) and ships them via `MetricsEvent::NodeReservoirDump`.
      End-of-run is detected by comparing `cx.time()` to
      `run_duration` inside `flush_summary`.

    Live counters (`total_first_seen`, `superseded_count`) remain
    `AtomicUsize` — bumped on the worker thread + the aggregator
    respectively, read by the CLI progress line.
    `MetricsHandle::finalize_remaining` sends `FinalizeAndShutdown`,
    joins the aggregator, drops the run-time writer-tx clone, then
    joins the writer threads (one per Parquet file — see invariant 29).

    History note: don't put per-MsgId state back on the worker
    threads; the previous `scc::HashMap` design contended on bucket
    locks. The single-threaded aggregator is the win.
15. **`RunCfg.threads: Option<usize>`** controls executor parallelism.
    `None` = NeXosim default (all cores); `Some(n)` = `with_num_threads(n)`.
    The CLI `--threads N` flag overrides the config value.
16. **`TopologyCfg::FromCsv`** loads a real LN snapshot via
    `topology::ln_data::load`. Pubkeys → `NodeId` and SCID strings →
    `Scid` are derived via xxhash64 (seeded from `cfg.seed`). On
    collision the loader returns `Err`; the user changes `cfg.seed`.
    `[channels]` becomes optional in this mode (CSV provides the
    channel set).
17. **Peer-build rule for CSV loads** (`topology::synthetic::build_peer_graph`)
    is **per-node and additive**: each node's pass only adds edges it
    chose. Hubs that "reject" 100 of 200 counterparties may still end
    up with all 200 as peers because leaves will pick them back. This
    matches real LN; don't add a symmetric-AND filter.
18. **`OneShotSingle { node = N }` is a vertex INDEX**, not a NodeId.
    With CSV-loaded sparse u64 ids, addressing by hash would be
    awkward. The index resolves to a NodeId via the topology's
    insertion order — `node = 0` always means "first row of
    `node_list.csv`" (or NodeId 0 for synthetic).
19. **`TopologyCfg::FromCsv.k` is `KByAlgo { flooding, cln, lnd }`,
    not a single `usize`.** `build_peer_graph` resolves k per-vertex
    from the vertex's `NodeAlgo`. This means for `algo.kind = "mix"`,
    the per-vertex algo MUST be assigned BEFORE `build_peer_graph`
    runs — that's why `sim.rs` does the Mix algo assignment in step 2,
    between vertex creation (step 1) and peer-graph build (step 3).
    Don't try to combine those steps for FromCsv.
20. **Parquet replay re-hashes its own IDs through
    `topology::ln_data::hash_pubkey` / `hash_scid_string`.** The
    parquet trace ships pubkeys as 66-hex strings and SCIDs as `u64`;
    the loader MUST format the SCID via `to_string()` before hashing,
    since the CSV loader hashes the literal decimal string. Hashing a
    `u64` SCID's bytes directly would produce a different `Scid` and
    silently break registry lookups.
21. **Parquet event timing is anchored to the first row.** The first
    `first_seen_timestamp` becomes sim t=0; subsequent rows fire at
    `(this_us - first_us)` micros into the run. Rows are guaranteed
    ascending by the input, so the loader BREAKs the moment a row
    crosses `[run] duration_seconds` — don't add a sort step.
22. **Origination events are pre-scheduled into the executor's
    priority queue** via `simu.scheduler().schedule_event(deadline,
    event_id, msg)` before `drive_simulation` starts stepping. The
    driver then only does `step_until` + progress prints — it does
    NOT call `process_event` per event. Don't re-introduce
    per-event injection in the driver loop; with dense parquet
    traces it makes every event a worker-pool synchronisation
    barrier. Caveat: `Scheduler::schedule_event` rejects deadlines
    `<= current sim time`, so events with `delay = 0` get bumped
    +1 ns at scheduling time (sub-ms latency model means this is
    invisible).
23. **`WireMessage::Batch` is `Arc<GossipBatch>`**, where
    `GossipBatch` is per-kind-partitioned
    (`chan_updates / node_anns / chan_anns: Vec<Gossip>`). The
    receiver takes each kind's `NodeState` `RwLock` exactly once per
    batch (vs. once per gossip), and the per-recipient broadcast
    clones become refcount bumps instead of full Vec allocations.
    Build via `GossipBatch::from_mixed` (Cln/Lnd ticks) or
    `from_mixed_for_kind` (sketch replies). The `serde` workspace
    dep needs the `rc` feature enabled for derive-Serialize/Deserialize
    on the Arc to compile.
24. **Per-node dedup state lives in `Arc<NodeState>`** (see [`state.rs`]),
    NOT inline on the model. Each node holds its own
    `SharedNodeState` plus a `Vec<SharedNodeState>` of just its
    direct peers (aligned with its per-peer Outputs). The `NodeState`
    shards its three maps under three independent `RwLock`s — a
    `chan_updates` write never blocks a `node_anns` reader. The
    sketch-protocol diff path uses [`state::compute_diff`] which
    acquires both sides' kind-specific locks in NodeIdx-min-first
    order to avoid deadlock; never violate that order. A DashMap
    variant was tried and reverted — per-key shard locking added
    ~2.2× wall-time overhead on sketch mode because compute_diff
    touches thousands of keys per call and each shard acquire is an
    atomic op, whereas one map-wide read lock is acquired once.
25. **Each node has a `Vec<Output<WireMessage>>` of per-peer
    Outputs**, one Output per directed edge in the peer graph. Each
    Output has exactly one `connect` target, so a `send` on one
    Output reaches exactly one peer — broadcasts are explicit
    `for out in &mut self.outputs { out.send(msg).await; }` loops.
    `peer_id_to_local: HashMap<NodeId, usize>` maps a peer's NodeId
    to its index in `outputs` / `peer_states` / `peer_ids` — used by
    sketch-reply paths and any future per-peer message routing.
26. **`SketchNode` runs set-reconciliation across all three
    `SketchKind`s.** Each `(node, peer)` pair has its own periodic
    ticker at a deterministic per-peer offset within
    `(0, stagger_ms]`; each tick fires THREE `Sketch`es in
    sequence (chan_updates, node_anns, chan_anns) at their own
    configurable capacities. The receiver computes the per-kind
    symmetric diff via `state::compute_diff`, which returns BOTH
    strict-difference counts (used for capacity-overflow check +
    metrics) AND newer-only Gossips (used for the wire reply).
    Stale-side entries — items where this side has an OLDER
    timestamp — are NEVER sent in the reply Batch; they're
    superseded by definition. Reply Batches are
    `Arc<GossipBatch>` so receivers can take each per-kind
    `RwLock` exactly once per batch. Sketch nodes do NOT fan-out
    gossip on `recv`. The sketch reply needs `.await` so it goes
    through a `schedulable!` helper with a 1-ns delay (satisfies
    NeXosim's "deadline strictly in future" rule).
27. **`AlgoCfg::Sketch` and `NodeAlgoKind::Sketch` carry three
    capacities** (`capacity_chan_updates`, `capacity_node_anns`,
    `capacity_chan_anns`), not a single `capacity`+`sketch_kind`
    pair. The defaults (512 / 64 / 64) reflect typical real-LN
    diff sizes — chan_updates dominates traffic so it gets the
    biggest sketch.
28. **`Gossip::derive_id` is the SOLE source of `MsgId`.** No
    sequential counters anywhere — every `Gossip` (originated by
    parquet/poisson/oneshot, or synthesised by
    `state::compute_diff` for a sketch reply) computes its `id`
    via `xxhash3_64` over the per-kind identity tuple. The
    originate paths in each node call `originate_stamp` which
    re-derives the id AFTER the timestamp has been stamped, so
    originator-side and downstream-sketch-side both converge on
    the same MsgId. Don't reintroduce `next_id: u32` counters in
    event sources — that's the bug that broke per-message
    coverage metrics for sketch protocol.
29. **Stats output is six Parquet files, one writer thread per file.**
    `stats_writer::spawn` opens one `crossbeam_channel::bounded` per
    output file (`msg_stats`, `node_counters`, `node_reservoirs`,
    `overflow_events`, `run_meta`, `node_pubkey`) and spawns one OS
    thread per channel. Arrow encoding + ZSTD compression therefore
    parallelise across files; a stalled writer only backs up its own
    producer code path. Hot files use `ZstdLevel(1)` for throughput;
    cold files use the default level. `RowSender` is a wrapper holding
    one `Sender<R>` per file and dispatches via a single-branch enum
    `match` on `WriterRow`. `Writer::close` drops all 6 senders, then
    joins all 6 threads.

## Common pitfalls

- **Bash loses cwd between calls.** Either chain with `cd … && …` in a
  single Bash call, or use absolute paths. Hit this several times.
- **`pgrep -f` matches the parent shell.** For finding the lnconv
  binary, use `pgrep -x lnconv` (exact match). With `-f`, the bash
  invoking it also matches and you get the wrong PID.
- **IDE diagnostics are usually stale right after a Write/Edit.** Trust
  `cargo build` over the diagnostic stream. The diagnostics stream
  often lags by one or two file mutations.
- **`cargo build` from outside the workspace fails with "could not find
  Cargo.toml".** Run from inside `lnconv-sim/` (or chain with `cd`).
- **`tail -N` on long-running outputs truncates the head.** When grepping
  progress lines from a sim, write to a file and grep that, don't pipe
  through `tail` first.
- **Never run `step_until` past the `mix-poisson-large` deadline
  without a memory monitor.** That config can use tens of GB of RSS;
  the user has watched it climb. Run with `pgrep -x lnconv` +
  `/proc/$pid/status:VmRSS` polling alongside.
- **NeXosim's `Output::send` is broadcast across that Output's
  connections.** Each node holds a `Vec<Output<WireMessage>>` with
  one Output per peer and exactly one `out.connect(target, &mbox)`
  per Output (set up in `sim.rs`), so in practice each `send` is
  unicast to one peer — but the broadcast semantics still apply if
  you ever multi-`connect` an Output. Per-peer routing goes through
  `peer_id_to_local`.
- **Schedulables can take `&Context<Self>` as a second arg.** The
  README's add-an-algorithm section assumes this; LndNode's `tick` uses
  it to schedule trickled batches. The first round of LND missed this
  and had to be redone.
- **`Cargo.toml` has both workspace and per-crate dep blocks.** Add
  new deps to both (workspace first, then `dep.workspace = true` in
  the crate that needs them). Notable workspace deps: `petgraph`
  (topology graphs), `csv` + `twox-hash` (CSV loader + xxhash3_64),
  `thiserror` (loader error types), `parquet` + `arrow-array` +
  `arrow-schema` + `glob` (parquet I/O — pin parquet/arrow-array to
  the same major), `crossbeam-channel` (writer/aggregator queues),
  and `duckdb` with `bundled` + `parquet` features (CLI report).
- **Markdown lint warnings on the plan file are cosmetic** — ignore
  `MD040`, `MD031`, `MD060` etc. when writing plans.

## Verification protocol

After any non-trivial change, run at least:

```bash
cd /home/jhb/2025/plebfi/lnconv-paper-sim/lnconv-sim
cargo build --release
./target/release/lnconv -c configs/flooding-smoke.toml | tail -10
./target/release/lnconv -c configs/cln-smoke.toml      | tail -10
./target/release/lnconv -c configs/lnd-smoke.toml      | tail -10
./target/release/lnconv -c configs/mix-smoke.toml      | tail -10
./target/release/lnconv -c configs/poisson-smoke.toml  | tail -15
./target/release/lnconv -c configs/poisson-tiny-pool.toml | tail -15
```

Expected p100 reference values (current as of this write-up):

| Config | p100 | Notes |
|---|---|---|
| `flooding-smoke` | ~400 ms | n=1000, k=8, latency=100 ms |
| `cln-smoke` | ~1.38 s | originator skips first stagger wait |
| `lnd-smoke` | ~1.69 s | OneShotSingle, trickle never engages |
| `mix-smoke` | ~1.69 s | OneShotSingle |
| `poisson-tiny-pool` | ~1.84 s | with `[channels].count = 50`, supersession kills some msgs |
| `ln-snapshot-flooding` | ~400 ms | real LN snapshot, peers diameter ≈ 5, latency 100 ms |
| `ln-snapshot-parquet-flooding` | ~500 ms p100 | real LN snapshot + 5-min parquet replay (~1700 events kept of ~65k scanned in window; ~21% SCID drift expected) |

If these drift more than ~10% without an obvious algorithmic reason,
something regressed.

For memory-sensitive changes, also run `mix-poisson-large.toml` for ~60s
sim time and watch RSS. Plateau is acceptable; linear growth at GB/min
means something is leaking.

## Cargo workspace shape

- `crates/lnconv-core/` — the library. Most edits land here.
- `crates/lnconv-cli/` — clap entry point. Thin; reporting only.
- `Cargo.toml` (workspace) holds shared dep versions; per-crate
  `Cargo.toml`s use `dep.workspace = true`.
- `examples/diameter.rs` — `cargo run --release --example diameter -p
  lnconv-core` prints BFS diameters for the topologies you'd configure.

## When in doubt

- Read the most recent **plan file** under `~/.claude/plans/`. Plan
  files are per-task design notes that accumulate over time — each
  records the rationale for one round of changes, not a global
  blueprint. Sort by mtime to find the latest, or check the plan path
  recorded in the active session.
- The **Go reference simulator** is at `..` (parent dir). Files of
  interest: `lnsim/node.go` for stagger semantics, `lnsim/setreconcil.go`
  for the (mocked) reconciliation algorithm. Use as a behavioral
  reference, not a port target.
- **BOLT 7** is at <https://github.com/lightning/bolts/blob/master/07-routing-gossip.md>.
  Section 4 ("Channel updates") and the timestamp dedup rules are the
  spec we're approximating.
