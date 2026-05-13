# Performance measurement

Two layers, used together:

- **Binary-level profiling** (samply / cargo-flamegraph) — answers
  "where is wall-clock going on a real sim run?". Run on the
  `ln-snapsnap-sketch` workload (real LN snapshot, ~12k nodes,
  parquet-replay traffic, sketch protocol).
- **Function-level microbenchmarks** (Divan) — answers "did a
  specific change to `compute_diff` actually help, and by how much?".

The release profile in [Cargo.toml](Cargo.toml) already carries
`debug = "line-tables-only"`, so the binary symbolicates without a
separate build.

## Binary-level profiling with samply

### One-time setup

```bash
cargo install samply
# Linux: samply uses perf_event_open, which needs paranoia <= 1.
sudo sysctl -w kernel.perf_event_paranoid=1
```

### Record a profile

The default full-hour `ln-snapsnap-sketch.toml` is slow to iterate
on; use the 5-minute variant for the profiling loop:

```bash
cargo build --release -p lnconv-cli
mkdir -p perf

samply record -o perf/ln-snapsnap-sketch.json.gz \
  ./target/release/lnconv --config configs/ln-snapsnap-sketch-short.toml
```

`samply record` opens the Firefox profiler UI in a browser when the
run finishes. The `.json.gz` is portable — share with a teammate or
re-open at https://profiler.firefox.com.

When the inner-loop hotspot is clear, switch to the full
`ln-snapsnap-sketch.toml` for the headline numbers.

### What to look for

In the flame graph, expect:

- `compute_diff` / `diff_chan_updates` / `diff_node_anns` /
  `diff_chan_anns` under `SketchNode::handle_sketch`.
- `IntMap::get` / hashbrown swisstable probe under those.
- `originate_stamp` and the `absorb_*` family under the recv path.
- The metrics aggregation path (`record_first_seen` and the
  `scc::HashMap` bucket lock) — called out as a separate
  bottleneck in the sketch absorb code.

### Alternative: cargo-flamegraph

```bash
cargo install flamegraph
cargo flamegraph --release --bin lnconv \
  -- --config configs/ln-snapsnap-sketch-short.toml
```

Produces a self-contained SVG (less interactive than samply, easier
to attach to a PR).

## Function-level benchmarks with Divan

Defined in [crates/lnconv-core/benches/diff.rs](crates/lnconv-core/benches/diff.rs).
Covers each `SketchKind` over `(map_size, diff_fraction)` cells. The
`with_inputs` setup builds synthetic `NodeState` pairs outside the
timed section so what's measured is purely `compute_diff`.

```bash
cargo bench -p lnconv-core --bench diff
```

The output table reports median, fastest, slowest, p99, samples, and
allocations/iter — the alloc columns come from `divan::AllocProfiler`,
which is wired as the bench harness's global allocator.

### Comparing before/after a change

Divan doesn't ship a built-in baseline-comparison (criterion does),
so use git stash for a simple manual comparison:

```bash
# Baseline
git stash
cargo bench -p lnconv-core --bench diff 2>&1 | tee /tmp/before.txt

# Change
git stash pop
cargo bench -p lnconv-core --bench diff 2>&1 | tee /tmp/after.txt

diff -u /tmp/before.txt /tmp/after.txt
```

### When to add benches for a new helper

If a change targets a function not currently covered (e.g., a new
cache lookup path), add a new `#[divan::bench]` function in
`benches/diff.rs` modeled on the existing `diff_chan_updates`
pattern. Reuse the `make_pair_*` helpers — they're deterministic
across runs (seeded `ChaCha8Rng`).

## Future work

- **Callgrind / gungraun** for reproducible instruction-count
  regression gating in CI. Most valuable once there is a CI
  pipeline that can run Valgrind (10–50× slowdown, but
  measurement-noise-free).
- **`tracing` + `tracing-flame`** spans inside the sim if a
  per-event timeline is needed (different question from "what's
  hot?" — closer to "what's the order of operations on a single
  message?").

## Jemalloc

Use this env. var. at process start (can be embedded in the binary as well):

`MALLOC_CONF=thp:always,metadata_thp:auto`

Embedded version:

```sh
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"metadata_thp:auto\0";
```

Needs OS-level config:

```sh
cat /sys/kernel/mm/transparent_hugepage/enabled 

[always] madvise never
```

Improvements from this are untested.
