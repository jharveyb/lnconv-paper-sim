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
|---|---|
| `flooding-smoke.toml` | Flooding on n=1000 random k=8 graph, single-origin one-shot |
| `flooding-large.toml` | Flooding on n=20000 (scale check) |
| `cln-smoke.toml` | CLN-style 1s stagger, single-origin |
| `cln-large.toml` | CLN-style 60s stagger, n=20000, every node originates one message |
| `lnd-smoke.toml` | LND-style stagger + trickle |
| `lnd-all.toml` | LND with `OneShotAll` so trickle actually engages |
| `mix-smoke.toml` | 70% LND / 30% CLN heterogeneous population |
| `poisson-smoke.toml` | Poisson stream of 7 messages/sec from random nodes |

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
kind = "k_regular"              # currently the only generator
n = 1000                        # number of nodes
k = 8                           # exact degree of every node (true random regular)

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
```

### Algorithm semantics

| Algorithm | What each node does |
|---|---|
| **Flooding** | On first sight of a new message, schedule a forward to all peers after `latency.ms`. |
| **Cln** (c-lightning-style) | Pending queue per node. Periodic tick every `stagger_ms` drains the queue and broadcasts everything as a single batch. No trickle, no chunking. |
| **Lnd** (LND-style) | Pending queue per node. Periodic tick splits into chunks of `min_batch_size`; first chunk goes immediately, subsequent chunks at `+i·trickle_ms`. |
| **Mix** | Per-node assignment of Cln/Lnd from a fraction list; deterministic shuffle by `seed`. Connections between mixed nodes work because both `recv` methods take the same `WireMessage` type. |

Each stagger node samples its first-tick offset uniformly in
`(0, stagger_ms]` from the seeded RNG. This avoids same-instant tick
cascades and makes per-hop wait time average ~`stagger_ms / 2`.

### Event sources

| Kind | Behavior |
|---|---|
| `one_shot_single { node }` | One message originated by `node` at `t=0`. |
| `one_shot_all` | Every node originates exactly one message at `t=0`. |
| `poisson_random { rate_per_sec, size_bytes }` | Exponential inter-arrival with mean `1/rate_per_sec`; each message originated by a uniformly-random node; runs until `duration_seconds`. |

---

## Output

At sim start the binary prints the resolved config, the BFS-derived
topology stats, and (for mixed populations) the per-algorithm node count.

While running, every `progress_interval_seconds` of simulated time it
prints:

```
[t=  120.0s | wall=  13.9s] first_seen= 154995811 (+ 52539784,  12321536/wall_s)
```

- `t` — current simulated time in seconds.
- `wall` — real elapsed seconds since the sim started.
- `first_seen` — total `(node, message)` first-seen events recorded so far.
- `+delta` and `rate` — events accumulated since the previous print.

When the run finishes, it prints per-message stats followed by
mean-percentile aggregates across all messages that reached 100% coverage:

```
msg     covg     covg%   p 5    p10    p25    p50    p75    p90    p99   p100
0       1000   100.0%   ...

aggregate over 352 messages that reached 100% coverage:
  p 25: mean = 1.07s
  p 50: mean = 1.22s
  ...
  p100: mean = 1.84s
```

Each percentile column is the time, measured from the first node to see
the message, until that fraction of nodes had seen it. **p100 is the full
network-convergence time for that message.** For a quick gut check:

- For flooding, expect p100 ≈ `topology.diameter × latency.ms`.
- For stagger algorithms, expect p100 ≈ `topology.mean_path_length × stagger_ms / 2`.

---

## Architecture

### Two layers

```
┌────────────────────────────────────────────────────────┐
│  lnconv-cli  — clap binary, loads TOML, prints output  │
├────────────────────────────────────────────────────────┤
│  lnconv-core                                           │
│  ├─ sim.rs        chunked driver + population builder  │
│  ├─ node/         FloodingNode | ClnNode | LndNode     │
│  ├─ message.rs    Gossip + WireMessage (Single|Batch)  │
│  ├─ topology/     random k-regular + BFS metrics       │
│  ├─ events/       OneShot* + PoissonRandom             │
│  ├─ metrics.rs    per-node first-seen tracker          │
│  └─ config.rs     TOML schema (serde)                  │
├────────────────────────────────────────────────────────┤
│  NeXosim 1.0  — discrete-event executor + ports        │
└────────────────────────────────────────────────────────┘
```

### NeXosim primer (for LN folks new to discrete-event sim)

Each LN node is a NeXosim **`Model`**, an actor with:
- private mutable state (`HashSet<MsgId>` for dedup, pending queue),
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

### Topology

[`topology/synthetic.rs`](crates/lnconv-core/src/topology/synthetic.rs)
calls
[`rustworkx_core::generators::random_regular_graph`](https://docs.rs/rustworkx-core/latest/rustworkx_core/generators/fn.random_regular_graph.html)
to produce a true k-regular random graph (every node has exactly `k`
neighbours, no parallel edges, no self-loops). The result is converted to
a flat `Vec<Vec<u32>>` adjacency list because that's what the wiring loop
in `sim.rs` iterates once at startup. After init the graph is never
touched; runtime cost is dominated by message dispatch.

[`topology/metrics.rs`](crates/lnconv-core/src/topology/metrics.rs)
computes degree stats, BFS-derived diameter and mean path length, and
connectedness. Exact for `n ≤ 2000`; sampled (200 random sources) for
larger graphs.

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
- **Metrics are mutex-shared.** The `MetricsHandle` is an
  `Arc<Mutex<...>>` written from every node. At ~12M events/wall-second
  this hasn't been a bottleneck so far; if it becomes one, switch to
  per-node buffers + post-run merge.

---

## Repository layout

```
lnconv-sim/
├── Cargo.toml                     workspace
├── README.md
├── configs/                       runnable TOML configs
├── crates/
│   ├── lnconv-core/
│   │   ├── examples/diameter.rs   topology BFS sweep
│   │   └── src/
│   │       ├── lib.rs
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
