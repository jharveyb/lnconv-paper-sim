# lnconv-sim

A discrete-event simulator for **Lightning Network gossip-protocol
dynamics**. It studies how channel updates propagate across a peer-to-peer
graph under different gossip strategies (flooding, c-lightning-style
batching, LND-style staggered batching, future: inventory + set
reconciliation), without modelling payments or fee policies.

The simulator is built on
[NeXosim](https://github.com/asynchronics/nexosim), a Rust discrete-event
framework with a parallel async executor and typed message-passing ports —
a clean fit for "every node is an actor with one input, one fan-out."

It is the Rust successor to the Go simulator in the parent
[`lnconv-paper-sim`](..) repository, narrowed to gossip-only and rebuilt
to support **mixed per-node algorithms**, **rate-based event streams**,
and **realistic large-scale topologies**.

---

## Quick start

```bash
# Requires Rust 1.88+
cargo build --release

./target/release/lnconv -c configs/flooding-smoke.toml
```

Available smoke configs in [`configs/`](configs):

| Config | What it tests |
| --- | --- |
| `flooding-smoke.toml` | Flooding on n=1000 random k=8 graph, single-origin one-shot |
| `flooding-large.toml` | Flooding on n=20000 (scale check) |
| `cln-smoke.toml` | CLN-style 1s stagger, single-origin |
| `cln-large.toml` | CLN-style 60s stagger, n=20000, every node originates one message |
| `lnd-smoke.toml` | LND-style stagger + trickle |
| `lnd-all.toml` | LND with `OneShotAll` so trickle actually engages |
| `mix-smoke.toml` | 70% LND / 30% CLN heterogeneous population |
| `poisson-smoke.toml` | Poisson stream of 7 messages/sec from random nodes |
| `poisson-tiny-pool.toml` | Poisson with a tiny `[channels].count = 50` to force BOLT 7 supersession |
| `mix-poisson-large.toml` / `mixed-poisson-large.toml` | n≈20k, mix population, hour-long Poisson — long-run / memory stress |
| `ln-snapshot-flooding.toml` | Real LN snapshot from `init_data/` (~12k nodes, ~42k channels), flooding from one origin |
| `ln-snapshot-parquet-flooding.toml` | Real LN snapshot + parquet replay of mainnet gossip traffic for the first 5 minutes of the trace |

The example `cargo run --release --example diameter -p lnconv-core` prints
BFS-derived diameters and mean path lengths for a sweep of `(n, k)` —
useful for predicting propagation timings before kicking off a full run.

---

## Configuration reference

A run is fully described by a TOML file with five sections plus a global
`seed`. Annotated example:

```toml
seed = 1                        # ChaCha8 seed; drives topology, phases, Poisson stream

[topology]
kind = "k_regular"              # one of: "k_regular" | "from_csv"
n = 1000                        # number of nodes
k = 8                           # exact degree of every node (true random regular)

# Or, to load a real LN snapshot:
# [topology]
# kind = "from_csv"
# nodes_csv = "init_data/node_list.csv"
# channels_csv = "init_data/channel_list.csv"
# k = 5                         # peer-build threshold + sparse-node fallback

[channels]
count = 2000                    # number of SCIDs in the LN graph; each gets two
                                # directions, each owned by a random node.
                                # Use >= n so every node owns at least one channel.
                                # OPTIONAL when topology.kind = "from_csv" — the
                                # CSV provides the channel set.

[latency]
dist = "constant"               # currently the only distribution
ms = 100                        # one-way per-edge latency (only used by Flooding for now)

[algo]
kind = "lnd"                    # one of: "flooding" | "cln" | "lnd" | "mix"
stagger_ms = 90000              # stagger interval (CLN/LND only)
trickle_ms = 5000               # inter-batch trickle (LND only)
min_batch_size = 10             # batch chunk size (LND only)

# For kind = "mix", instead of the per-algo fields above, list a population:
# [[algo.population]]
# fraction = 0.7
# [algo.population.algo]
# kind = "lnd"
# stagger_ms = 90000
# trickle_ms  = 5000
# min_batch_size = 10
# [[algo.population]]
# fraction = 0.3
# [algo.population.algo]
# kind = "cln"
# stagger_ms = 60000

[event]
kind = "poisson_random"         # one of: "one_shot_single" | "one_shot_all" | "poisson_random"
rate_per_sec = 7.0              # only for poisson_random
# size_bytes = 1024             # optional, default 1024

[run]
duration_seconds = 600          # simulated time window
mailbox_capacity = 1024         # per-node NeXosim mailbox slots; raise if you see Deadlock errors
progress_interval_seconds = 10  # 0 to silence in-flight progress prints
# threads = 24                  # optional; pin executor pool to N. Defaults to all
                                # available logical cores. CLI --threads N overrides.
```

The CLI also accepts `-t N` / `--threads N` to override `[run].threads`
(useful for thread-scaling experiments without editing the config).

### Algorithm semantics

Every node — regardless of algorithm — keeps a per-node `lngraph:
HashMap<(Scid, Direction), u32>` of the latest gossip timestamp it has
seen for each channel. On `recv`, an arrival with timestamp ≤ stored is
dropped (BOLT 7 supersession); strictly newer arrivals update the entry
and re-broadcast.

| Algorithm | What each node does |
| --- | --- |
| **Flooding** | On a fresh `(scid, direction, timestamp)`, schedule a forward to all peers after `latency.ms`. Originated messages broadcast immediately. |
| **Cln** (c-lightning-style) | Forwarded gossip waits in a `pending` queue and is sent in one big `Batch` per `stagger_ms` tick. **Originated messages bypass the queue and are broadcast immediately as `Single`** (matches CLN's "local updates aren't held by the broadcast window"). |
| **Lnd** (LND-style) | Same per-tick batching as Cln, but pending is split into chunks sized by `calculate_sub_batch_size(stagger_ms, trickle_ms, min_batch_size, pending_len)` so all chunks fit inside the stagger window. Concretely: `chunk = max(min_batch_size, ceil(pending_len * trickle_ms / stagger_ms))`. With stagger=90s, trickle=5s, pending=360, min=10 → chunk=20 (18 sub-batches), not 10 (36 sub-batches). First chunk goes immediately on tick; subsequent chunks at `+i·trickle_ms`. **Originated messages are inserted at the FRONT of `pending`**, so they ride out in the first chunk ahead of any forwarded traffic. |
| **Mix** | Per-node assignment of Cln/Lnd from a fraction list; deterministic shuffle by `seed`. Connections between mixed nodes work because both `recv` methods take the same `WireMessage` type. The chosen kind for each node is stored as `NodeAlgo` on the `peers` graph vertex. |

Each stagger node samples its first-tick offset uniformly in
`(0, stagger_ms]` from the seeded RNG. This avoids same-instant tick
cascades and makes per-hop wait time average ~`stagger_ms / 2`.

### Event sources

For every event the originator is determined by the **channel registry**
(`channels.rs`) — the `(scid, direction)` is sampled, then the registry
looks up which node owns that channel side. Streams never "pick a node
at random"; they pick a channel.

| Kind | Behavior |
| --- | --- |
| `one_shot_single { node }` | One message at `t=0`, on the first `(scid, direction)` that `node` owns. Skipped (with a warning) if `node` owns no channels. |
| `one_shot_all` | Every node that owns at least one channel originates one message at `t=0`, using its first owned `(scid, direction)`. |
| `poisson_random { rate_per_sec, size_bytes }` | Exponential inter-arrival with mean `1/rate_per_sec`; each event picks a uniformly-random `(scid, direction)` and uses its owner as the originator. Runs until `duration_seconds`. |
| `parquet_replay { path }` | Replay events from a real-world ZSTD-compressed parquet capture. See **Replaying real-world traffic from parquet** below. |

---

## Output

At sim start the binary prints the resolved config, the BFS-derived
topology stats, and (for mixed populations) the per-algorithm node count.

While running, every `progress_interval_seconds` of simulated time it
prints:

```sh
[t=  120.0s | wall=  13.9s] first_seen= 154995811 (+ 52539784,  12321536/wall_s)
```

- `t` — current simulated time in seconds.
- `wall` — real elapsed seconds since the sim started.
- `first_seen` — total `(node, message)` first-seen events recorded so far.
- `+delta` and `rate` — events accumulated since the previous print.

When the run finishes the binary prints, in order:

1. `simulation finished: N distinct messages` and `total first-seen events: …`.
2. `superseded: K of N …` if any messages were killed mid-spread by a
   newer `(scid, direction)` version (BOLT 7 supersession).
3. A per-message table (first 5 messages) showing each percentile column;
   `--` means the message never reached that absolute coverage.
4. A **coverage distribution**: how many messages reached each tier.
5. A **time-to-reach-coverage distribution per tier**: for each of
   25%, 50%, 75%, 100%, the distribution *across messages* of the time
   that message took to reach that coverage.

```sh
msg     covg     covg%   p 5    p10    p25    p50    p75    p90    p99   p100
0       1000   100.0%   ...

coverage distribution (messages reaching >= X% of 1000 nodes):
  >=  25% (>=   250 nodes):    365 / 403 (90.6%)
  >=  50% (>=   500 nodes):    361 / 403 (89.6%)
  >=  75% (>=   750 nodes):    358 / 403 (88.8%)
  >= 100% (>=  1000 nodes):    347 / 403 (86.1%)

time to reach 25% coverage (>= 250 of 1000 nodes): 365 of 403 messages reached it
  min:    427ms     p50:    1.05s     mean:    1.08s
  p 5:    584ms     p75:    1.34s     max:     1.84s
  p25:    817ms     p95:    1.59s

time to reach 100% coverage (>= 1000 of 1000 nodes): 347 of 403
  min:   1.17s      p50:    1.82s     mean:    1.84s     max: 2.63s
```

Each per-message percentile is the time, measured from the first node to
see the message, until that fraction of *all `n` nodes* had received it
(absolute — not relative to per-message coverage). When a message is
killed mid-spread by BOLT 7 supersession, its lower percentiles are
still defined while the higher ones come back as `--`. **p100 is the
full network-convergence time for that message.** For a quick gut check:

- For flooding, expect p100 ≈ `topology.diameter × latency.ms`.
- For stagger algorithms, expect p100 ≈ `topology.mean_path_length × stagger_ms / 2`.

---

## Architecture

### Two layers

```sh
┌────────────────────────────────────────────────────────┐
│  lnconv-cli  — clap binary, loads TOML, prints output  │
├────────────────────────────────────────────────────────┤
│  lnconv-core                                           │
│  ├─ sim.rs        chunked driver + population builder  │
│  ├─ node/         FloodingNode | ClnNode | LndNode     │
│  ├─ message.rs    Gossip + WireMessage (Single|Batch)  │
│  ├─ channels.rs   (scid, direction) -> owner registry  │
│  ├─ topology/     petgraph: peers UnGraph + channels   │
│  │                 DiGraph + dijkstra-derived metrics  │
│  ├─ events/       OneShot* + PoissonRandom             │
│  ├─ metrics.rs    BOLT-7-aware first-seen tracker      │
│  │                 (scc-backed concurrent containers)  │
│  └─ config.rs     TOML schema (serde)                  │
├────────────────────────────────────────────────────────┤
│  NeXosim 1.0  — discrete-event executor + ports        │
└────────────────────────────────────────────────────────┘
```

### NeXosim primer (for LN folks new to discrete-event sim)

Each LN node is a NeXosim **`Model`**, an actor with:

- private mutable state (`lngraph` for BOLT 7 dedup, pending queue),
- typed input ports (methods marked by `#[Model]` like `recv` and `originate`),
- typed output ports (`Output<WireMessage>`) that broadcast to many peers.

Models communicate through **`Mailbox`es**. `mbox.connect(target_input, &dst_mbox)` wires
one node's output to another node's input. The executor advances
**simulated time** by popping the earliest event from a min-heap, runs the
recipient's input handler, and any new events scheduled inside that
handler land back in the heap.

What this gives us:

- **Deterministic ordering** for the same seed, regardless of how many
  cores the executor uses.
- **Free parallelism** — independent models advance concurrently.
- **Per-node periodic timers** via `Context::schedule_periodic_event`,
  used by Cln/Lnd for their stagger ticks.

### `WireMessage`: the shared wire type

```rust
enum WireMessage {
    Single(Gossip),      // flooding, originate, intermediate forwards
    Batch(Vec<Gossip>),  // CLN/LND tick output
}
```

All node kinds expose `recv: fn(WireMessage, ...)`. That single shared
type is what lets a CLN node forward a `Batch` to an LND peer and vice
versa in a mixed population. Receivers iterate `.iter_gossips()`.

### Loading a real LN snapshot

[`init_data/`](init_data/) ships two CSVs from a January 2026 mainnet snapshot:

- `node_list.csv` — one column `pubkey`, ~12 000 rows.
- `channel_list.csv` — `scid, node_1, node_2`, ~42 000 rows.

To run against this data instead of a synthetic k-regular graph:

```toml
[topology]
kind = "from_csv"
nodes_csv = "init_data/node_list.csv"
channels_csv = "init_data/channel_list.csv"
enforce_hub_cap = false   # default; see below

[topology.k]
flooding = 5    # peer-degree target for flooding nodes
cln = 10        # CLN nodes target ~10 peers
lnd = 3         # LND nodes target ~3 peers
```

`k` is per-impl-type — different gossip algorithms target different
peer-degrees, and a Mix population uses each vertex's assigned algo to
pick the right `k`. For homogeneous-flooding/cln/lnd configs, only the
matching field is read; the other two are still required by the schema.

`enforce_hub_cap` changes how peer-graph edges into a hub
(`c > 100` channel counterparties) are handled:

| value | behavior | snapshot peers max-degree |
| --- | --- | --- |
| `false` (default) | Hub picks 100 counterparties; leaves can still pick the hub back, so the hub may end up with all `c > 100` peers. Matches "real LN: a leaf wants to peer with its only channel partner". | ≈ 1700 |
| `true` | Hub pre-commits to its 100 picks; any incoming peer-edge request from outside that set is dropped. Each leaf whose hub-edge got dropped gets one *replacement stranger* in a phase-3 top-up, so its peer-degree stays close to its phase-1 plan. | ≈ 100 (hub itself); other nodes preserved |

Enabling the cap reshapes the peer-graph (hubs lose their fanout) but
keeps the **total edge count and mean-degree the same** as the uncapped
case, because every dropped edge is replaced with a stranger. The
peer-graph diameter does grow (6 → 9 on the snapshot) since hubs no
longer shortcut across the network.

Pubkeys → `NodeId` and SCID strings → `Scid` are derived via xxhash64
(seeded from `cfg.seed`). Both ID types are u64, so birthday-collision
probability on this dataset is negligible (~5e-12 for nodes / ~2e-11
for channels). On the unlikely event of a collision the loader returns
an error naming both colliding inputs and the seed; bumping `cfg.seed`
reshuffles all derived IDs.

The channel graph is taken verbatim from the CSV (`node_1` is the
direction-0 owner, `node_2` is direction-1). The peer graph is then
built from it via the per-node rule:

- `c > 100` channel counterparties: keep 100 random ones as peers.
- `k <= c <= 100`: keep all + 1 random stranger.
- `c < k`: keep all + `k` random strangers.

Each node's pass only adds peer-edges incident to itself. A hub that
picks 100 of its 200 counterparties may still end up with all 200 as
peers because every leaf will still pick the hub back — this matches
real LN, where a leaf wants to peer with its only channel partner.

`OneShotSingle { node = N }` interprets `N` as the dense vertex
*index* (0..n) — for a CSV-loaded run, `node = 0` is whichever pubkey
is on the first row of `node_list.csv`. This makes the same config
field work for both synthetic and CSV-loaded topologies.

### Replaying real-world traffic from parquet

[`init_data/`](init_data/) also ships a ZSTD-compressed parquet
capture of mainnet LN gossip
(`compact_traffic_2026-03-08_2026-03-10.parquet`, 28 MB, 1.53 M rows
over ~3 days). The `parquet_replay` event source loads it and replays
every row at its original `first_seen_timestamp` cadence relative to
the first row.

```toml
[event]
kind = "parquet_replay"
path = "init_data/compact_traffic_*.parquet"  # glob ok
```

The first parquet row maps to sim t=0; rows past
`[run] duration_seconds` are dropped at load time (the loader
short-circuits the moment it crosses the window — no point reading the
rest of a 3-day file for a 5-minute run).

Three BOLT 7 message kinds are emitted (`Gossip.kind`):

| Kind | Per-row emission | Per-node dedup |
| --- | --- | --- |
| `node_announcement` (type=2) | One tuple. Origin = `xxhash3_64(seed XOR NODE_SUBSEED, orig_node)`. | `node_anns: HashMap<NodeId, u32>` — highest timestamp per origin wins. |
| `channel_announcement` (type=1) | TWO tuples at the same delay, one per `registry.owner(scid, 0)` and `(scid, 1)`. Mirrors BOLT 7 (both endpoints sign + gossip). | `chan_anns: HashSet<Scid>` — first arrival wins; receiver dedup collapses the second cascade after one hop. |
| `channel_update` (type=3) | One tuple. Direction round-robins per SCID (alternates 0,1,0,1 within each SCID's stream). The parquet does not carry the real BOLT 7 channel_flags direction bit; per-direction asymmetry metrics on parquet replays are therefore NOT meaningful. | `chan_updates: HashMap<(Scid, Direction), u32>` — same as before, highest timestamp wins. |

Pubkeys / SCIDs that aren't in the snapshot (~5 % drift between the
January and March datasets) are silently skipped at load time; the
loader prints a one-line summary like:

```sh
parquet replay: scanned 65536 rows, kept 1729 tuples (within 300s window),
  skipped 469 unknown SCIDs / 35 unknown pubkeys
```

The originator-side dedup (`chan_anns.insert(scid)`) on each node
prevents the "both endpoints emit" doubling from producing two
broadcast cascades — only the first endpoint's `originate` actually
broadcasts; the second's no-ops.

### Topology — two petgraph graphs sharing one vertex set

[`topology/graph.rs`](crates/lnconv-core/src/topology/graph.rs) defines
the `Topology` struct:

```rust
pub struct Topology {
    pub peers:    UnGraph<NodeMeta, ()>,   // who exchanges gossip with whom
    pub channels: DiGraph<(),       Scid>, // one directed edge per LN channel
}

pub struct NodeMeta {
    pub id:   NodeId,
    pub algo: NodeAlgo,                    // Flooding | Cln{...} | Lnd{...}
}
```

Both graphs share one NodeIndex space (insertion order matches
`NodeId 0..n`). The peer graph carries the per-vertex `(NodeId, NodeAlgo)`
metadata so wiring loops can read a node's algorithm directly off the
vertex weight instead of via a parallel `Vec<NodeAlgoKind>`.

`channels` is a *directed* graph with one edge per channel. The
direction encodes ownership structurally: an outgoing edge from `N`
carrying `Scid s` means `N` owns `(scid=s, direction=0)`; the same edge
seen from the destination is its `(scid=s, direction=1)` ownership. The
"directions are opposite-endpoint" invariant becomes impossible to
violate — there's only one edge to look at.

[`topology/synthetic.rs`](crates/lnconv-core/src/topology/synthetic.rs)
calls
[`rustworkx_core::generators::random_regular_graph`](https://docs.rs/rustworkx-core/latest/rustworkx_core/generators/fn.random_regular_graph.html)
to produce a true k-regular random graph (every node has exactly `k`
neighbours, no parallel edges, no self-loops) and copies its edges into
the `peers` graph. After init the graph is never mutated; runtime cost
is dominated by message dispatch.

[`topology/metrics.rs`](crates/lnconv-core/src/topology/metrics.rs)
computes degree stats and runs
[`petgraph::algo::dijkstra`](https://docs.rs/petgraph/latest/petgraph/algo/dijkstra/fn.dijkstra.html)
(unit edge weights → BFS distance) from each source to derive diameter,
mean path length, and connectedness. Exact for `n ≤ 2000`; sampled
(1000 random sources) for larger graphs.

### Channel registry

[`channels.rs`](crates/lnconv-core/src/channels.rs) builds a
`ChannelRegistry` once at sim init. For each of `[channels].count`
SCIDs, two distinct random nodes are chosen and each takes one direction
(`0` or `1`); a single directed edge is added to `topology.channels`
from the dir-0 owner to the dir-1 owner, carrying the SCID. The
registry caches `owner(scid, direction) -> NodeId` and
`channels_for(node) -> &[(Scid, Direction)]` for O(1) lookups from
event-stream generators. Channels and the peer graph stay independent
for now — a channel between u and v doesn't imply a peer edge.

### The chunked sim driver

`sim.rs::drive_simulation` repeatedly:

1. Computes the next "checkpoint": min of `deadline`, next scheduled
   gossip event, next progress-print time.
2. Calls `Simulation::step_until(checkpoint)` to advance.
3. Drains any scheduled origination events whose time is now past.
4. Prints a progress line if a progress-tick boundary was crossed.

This shape is what lets us mix:

- **Static one-shot events** (all events queued at `t=0`),
- **Time-spread streams** (Poisson, a Vec sorted by time),
- **Periodic progress output** without polluting NeXosim's event queue.

### Per-node tick phasing (subtle but important)

Every Cln/Lnd node samples a uniform offset in `(0, stagger_ms]` for its
first tick. Without this, all nodes' periodic ticks fire at the same
absolute times (e.g., 60000ms, 120000ms, …) and NeXosim sometimes
processes a `recv` *before* the recipient's tick at the same timestamp,
allowing a single message to cascade many hops in one time step. With
random phasing, each hop genuinely waits ~`stagger_ms / 2` on average.

---

## Adding a new algorithm

The shape is small and uniform. To add e.g. an `InventoryNode`:

1. **Create `node/inventory.rs`** with a struct holding state, an
   `Output<WireMessage>`, and the `MetricsHandle`. Mark it
   `#[derive(Default, Serialize, Deserialize)]` and skip non-serde fields
   with `#[serde(skip)]`. Implement under `#[Model]`:
   - `pub fn recv(&mut self, wire: WireMessage, cx: &Context<Self>)`
   - `pub async fn originate(&mut self, msg: Gossip, cx: &Context<Self>)`
   - `#[nexosim(init)]` setup (if periodic ticks are needed)
   - `#[nexosim(schedulable)]` helpers for delayed sends
2. **`pub mod inventory;`** in `node/mod.rs`.
3. **Add a variant** to `AlgoCfg` in `config.rs`.
4. **Add a `run_inventory` function** in `sim.rs` that mirrors
   `run_flooding` (or extend `run_stagger_population` if it shares the
   stagger-tick lifecycle).
5. **Drop a `configs/inventory-smoke.toml`** and verify against the smoke
   table.

The `WireMessage` enum already accommodates inventory framing — if your
algorithm needs new fields (e.g. inventory entries, reconciliation
sketches), add a new variant rather than a parallel wire type so existing
nodes can still parse what arrives.

---

## Known simplifications

- **Stagger algorithms ignore network latency.** Flooding uses
  `latency.ms` as its forward delay; Cln/Lnd send instantly on tick.
  Adding latency for stagger means wrapping each `out.send` in a delayed
  `do_send` (already done for flooding).
- **One topology generator (random k-regular).** Erdős–Rényi and
  Barabási–Albert are on the roadmap (rustworkx-core has both).
- **Mocked set reconciliation.** Not yet ported from the Go simulator —
  when added, it will compare sets directly rather than transporting real
  minisketch payloads (matches the Go behavior; can later swap in
  [`minisketch`](https://crates.io/crates/minisketch) bindings).
- **Metrics are concurrent but per-MsgId-serialised.** The
  `MetricsHandle` uses `scc::HashMap` for in-flight + per-channel state
  and `AtomicUsize` counters, so different MsgIds proceed in parallel
  with no global lock. But the n_nodes `record_first_seen` calls for a
  single MsgId all serialise on its bucket, capping per-MsgId
  parallelism. To unlock that we'd need atomic per-slot updates
  (`Vec<AtomicU64>` instead of `Vec<u64>`) — left for a follow-up.
- **Per-node `lngraph` is a `HashMap`.** At LN scale (~20k nodes,
  ~40k channels) the table fills as more channels see updates and the
  aggregate cost reaches several GB. A flat `Vec<u32>` indexed by
  `scid * 2 + direction` would cut per-entry overhead but pre-allocate
  the full table at startup — not yet wired up.
- **Per-node degree is uniform `k`.** Sampling `k` per node from a
  distribution (and per-impl-type) needs a degree-sequence-aware
  topology generator (Havel-Hakimi or configuration model);
  rustworkx-core 0.17 doesn't ship one, so it's a near-term TODO.

---

## Repository layout

```sh
lnconv-sim/
├── Cargo.toml                     workspace
├── README.md
├── configs/                       runnable TOML configs
├── crates/
│   ├── lnconv-core/
│   │   ├── examples/diameter.rs   topology BFS sweep
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── channels.rs        (scid, direction) -> owner registry
│   │       ├── config.rs          TOML schema
│   │       ├── events/            event-stream generators
│   │       ├── message.rs         Gossip + WireMessage
│   │       ├── metrics.rs         first-seen tracker + percentiles
│   │       ├── node/              one file per algorithm
│   │       ├── sim.rs             chunked driver + population setup
│   │       └── topology/          random regular + BFS metrics
│   └── lnconv-cli/
│       └── src/main.rs            clap entrypoint
└── target/                        cargo output
```

---

## References

- Original Go simulator: [`../`](..) — see its
  [`lnsim/node.go`](../lnsim/node.go) for the reference stagger and
  reconciliation semantics.
- [BOLT 7 — gossip protocol](https://github.com/lightning/bolts/blob/master/07-routing-gossip.md).
- [Erlay (Bitcoin set reconciliation)](https://arxiv.org/abs/1905.10518) — the design that inspires the LN reconciliation variant.
- [k-regular graph (Wikipedia)](https://en.wikipedia.org/wiki/Regular_graph).
- [NeXosim docs](https://docs.rs/nexosim/1.0.0/nexosim/) — read these
  before changing anything in `sim.rs` or `node/*.rs`.
