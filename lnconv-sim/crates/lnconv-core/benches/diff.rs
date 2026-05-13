//! Microbenches for `lnconv_core::state::compute_diff` and its three
//! per-kind helpers. Run with:
//!
//!   cargo bench -p lnconv-core --bench diff
//!
//! Each bench measures one cell of the matrix
//!   (SketchKind) × (map size) × (diff fraction).
//! `WhichSide::B` matches the production caller in
//! `SketchNode::handle_sketch` so the timings reflect what
//! sketch-replies actually pay.
//!
//! Divan's `AllocProfiler` is wired as the global allocator so the
//! output table also reports allocations / iter — useful for
//! validating that future map/cache changes don't regress the
//! allocation count.

use lnconv_core::message::SketchKind;
use lnconv_core::state::{
    NodeState, SharedNodeState, WhichSide, compute_diff, pack_cu_key,
};
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use twox_hash::xxhash3_64::Hasher as XX3Hasher;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// (map_size, diff_fraction) cells covered for every SketchKind.
///
/// `diff_fraction` is the *per-side* fraction of keys that are
/// disjoint, so the symmetric diff is `2 * diff_fraction * map_size`.
/// `0.0` exercises the all-intersection path; `0.001` matches the
/// observed steady-state sketch-smoke diff (~1 b_only / sketch);
/// `0.01` is closer to a "lagging peer" case.
const CASES: &[(u32, f64)] = &[
    (10_000, 0.0),
    // 100
    (10_000, 0.01),
    // 1000
    (10_000, 0.1),
    (80_000, 0.0),
    // 80
    (80_000, 0.001),
    // 160, above our estimate of differences
    (80_000, 0.0025),
    // 400
    (80_000, 0.005),
    // 800
    (80_000, 0.01),
];

#[divan::bench(args = CASES)]
fn diff_chan_updates(bencher: divan::Bencher, case: (u32, f64)) {
    let (size, frac) = case;
    bencher
        .with_inputs(|| make_pair_chan_updates(size, frac, 0xC0FFEE))
        .bench_values(|(a, b)| compute_diff(&a, &b, SketchKind::ChanUpdates, WhichSide::B));
}

#[divan::bench(args = CASES)]
fn diff_node_anns(bencher: divan::Bencher, case: (u32, f64)) {
    let (size, frac) = case;
    bencher
        .with_inputs(|| make_pair_node_anns(size, frac, 0xC0FFEE))
        .bench_values(|(a, b)| compute_diff(&a, &b, SketchKind::NodeAnns, WhichSide::B));
}

#[divan::bench(args = CASES)]
fn diff_chan_anns(bencher: divan::Bencher, case: (u32, f64)) {
    let (size, frac) = case;
    bencher
        .with_inputs(|| make_pair_chan_anns(size, frac, 0xC0FFEE))
        .bench_values(|(a, b)| compute_diff(&a, &b, SketchKind::ChanAnns, WhichSide::B));
}

// ---------------------------------------------------------------------
// Synthetic-state builders.
//
// Each returns a pair `(a, b)` of `SharedNodeState`. Construction:
//   * Generate `size` "shared" entries — same key + same ts on both
//     sides. (Shared count = `(1 - diff_frac) * size`.)
//   * Generate `size - shared` A-only entries (keys disjoint from B).
//   * Generate `size - shared` B-only entries (keys disjoint from A
//     AND from A-only).
//
// Both sides therefore hold exactly `size` entries; symmetric diff
// is `2 * diff_frac * size`. All RNG draws come from a seeded
// `ChaCha8Rng` so a bench rerun on the same hardware produces the
// same numbers.
// ---------------------------------------------------------------------

fn make_pair_chan_updates(
    size: u32,
    diff_frac: f64,
    seed: u64,
) -> (SharedNodeState, SharedNodeState) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let shared = ((1.0 - diff_frac) * size as f64) as u32;
    let unshared = size - shared ;
    let a = NodeState::new(0);
    let b = NodeState::new(1);

    // Non-overlapping indices
    let shared_entries = chan_update_entries(&mut rng, 0, shared).collect::<Vec<_>>();
    let a_only = chan_update_entries(&mut rng, size , size+unshared).collect::<Vec<_>>();
    let b_only = chan_update_entries(&mut rng, 2*size , (2*size)+unshared).collect::<Vec<_>>();
    // A-only
    {
        let mut m_a = a.chan_updates.write();
        let mut m_b = b.chan_updates.write();
        // Shared keys live in the low part of the SCID space [0..size).
        for (k, v) in shared_entries {
            m_a.insert(k, v);
            m_b.insert(k, v);
        }
        for (k, v) in a_only {
            m_a.insert(k, v);
        }
        for (k, v) in b_only {
            m_b.insert(k, v);
        }
    }
    (a, b)
}

fn make_pair_node_anns(
    size: u32,
    diff_frac: f64,
    seed: u64,
) -> (SharedNodeState, SharedNodeState) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let shared = ((1.0 - diff_frac) * size as f64) as u32;
    let unshared = size - shared ;
    let a = NodeState::new(0);
    let b = NodeState::new(1);
    let shared_entries = node_ann_entries(&mut rng, 0, shared).collect::<Vec<_>>();
    let a_only = node_ann_entries(&mut rng, size , size+unshared).collect::<Vec<_>>();
    let b_only = node_ann_entries(&mut rng, 2*size , (2*size)+unshared).collect::<Vec<_>>();
    {
        let mut m_a = a.node_anns.write();
        let mut m_b = b.node_anns.write();
        for (k, v) in shared_entries {
            m_a.insert(k, v);
            m_b.insert(k, v);
        }
        for (k, v) in a_only {
            m_a.insert(k, v);
        }
        for (k, v) in b_only {
            m_b.insert(k, v);
        }
    }
    (a, b)
}

fn make_pair_chan_anns(
    size: u32,
    diff_frac: f64,
    seed: u64,
) -> (SharedNodeState, SharedNodeState) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let shared = ((1.0 - diff_frac) * size as f64) as u32;
    let unshared = size - shared ;
    let a = NodeState::new(0);
    let b = NodeState::new(1);
    let shared_entries = chan_ann_entries(&mut rng, 0, shared).collect::<Vec<_>>();
    let a_only = chan_ann_entries(&mut rng, size , size+unshared).collect::<Vec<_>>();
    let b_only = chan_ann_entries(&mut rng, 2*size , (2*size)+unshared).collect::<Vec<_>>();
    {
        let mut m_a = a.chan_anns.write();
        let mut m_b = b.chan_anns.write();
        for (k, v) in shared_entries {
            m_a.insert(k, v);
            m_b.insert(k, v);
        }
        for (k, v) in a_only {
            m_a.insert(k, v);
        }
        for (k, v) in b_only {
            m_b.insert(k, v);
        }
    }
    (a, b)
}

/// 24-bit non-zero timestamp. 24 bits is wide enough to avoid
/// accidental ts collisions across the bench matrix; the non-zero
/// constraint mirrors `originate_stamp` (it stamps `now_secs >= 1`
/// after the first sim tick).
#[inline]
fn rand_ts(rng: &mut ChaCha8Rng) -> u32 {
    (rng.next_u32() & 0x00ff_ffff) | 1
}

#[inline]
fn rand_size(rng: &mut ChaCha8Rng) -> u16 {
    (rng.next_u32() % (u16::MAX as u32)) as u16

}

fn chan_update_entries(rng: &mut ChaCha8Rng, start: u32, end: u32) -> impl Iterator<Item = (u64, (u32, u16))> {
    let chan_update_seed = rng.next_u64();
    let scid_hash = move |i: u32| -> u64 {
        XX3Hasher::oneshot_with_seed(chan_update_seed, &i.to_le_bytes())
    };
    let chan_map_value = || -> Option<(u32, u16)> {
        Some((rand_ts(rng), rand_size(rng)))
    };
    let chan_map_values = std::iter::from_fn(chan_map_value);
    let scids = (start..=end).map(scid_hash)
    .enumerate().map(|(i, s)| pack_cu_key(s, (i & 1) as u8 ));
    scids.zip(chan_map_values)
}

// Exact same type hashmap as channel update
fn node_ann_entries(rng: &mut ChaCha8Rng, start: u32, end: u32) -> impl Iterator<Item = (u64, (u32, u16))> {
    chan_update_entries(rng, start, end)
}

fn chan_ann_entries(rng: &mut ChaCha8Rng, start: u32, end: u32) -> impl Iterator<Item = (u64, u16)> {
    let chan_update_seed = rng.next_u64();
    let scid_hash = move |i: u32| -> u64 {
        XX3Hasher::oneshot_with_seed(chan_update_seed, &i.to_le_bytes())
    };
    let chan_map_value = || -> Option<u16> {
        Some(rand_size(rng))
    };
    let chan_map_values = std::iter::from_fn(chan_map_value);
    let scids = (start..=end).map(scid_hash)
    .enumerate().map(|(i, s)| pack_cu_key(s, (i & 1) as u8 ));
    scids.zip(chan_map_values)
}