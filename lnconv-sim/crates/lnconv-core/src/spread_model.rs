//! Closed-form predictor for sketch-protocol coverage times.
//!
//! Sketch propagation is **pull-based**: a non-aware node N only learns
//! a new message when N's own per-peer ticker for an aware neighbour
//! fires (the reply path in [`handle_sketch`] sends `b_newer` — items
//! the receiver has that the sender doesn't — so information flows from
//! sketch-receiver back to sketch-sender).
//!
//! From this we derive the time-to-fraction-`f`-coverage as a closed
//! form by treating it as an **SI (susceptible-infected) epidemic** on
//! the peer graph with a graph-derived transmission rate. See the
//! module-level derivation in `[per_hop_seconds]`, `[coverage_bulk]`,
//! `[coverage_tail]`.
//!
//! Caveats: the bulk regime assumes mean-field mixing, which
//! under-predicts spread time on sparse graphs with structure (LN
//! snapshot lands ~1.2× the prediction). The empirical
//! [`SPARSE_FUDGE`] factor matches observed runs on the bundled LN
//! snapshot but should be re-calibrated for other topologies.

/// Sparse-graph correction. Mean-field mixing over-predicts spread
/// speed by ~20% for the bundled LN snapshot; multiply the bulk model
/// by this to match observed sim runs. Re-fit when running on a
/// substantially different topology.
pub const SPARSE_FUDGE: f64 = 1.2;

/// Per-hop wait time for a node with `k_aware` aware neighbours.
///
/// Derivation: each aware peer has an independent uniform phase in
/// `(0, stagger]` for its ticker. Order-statistics: the minimum of
/// `k_aware` i.i.d. `Uniform(0, T)` samples has expectation
/// `T / (k_aware + 1)`. The non-aware node receives the message via
/// the first ticker to fire among its aware peers.
pub fn per_hop_seconds(stagger_secs: f64, k_aware: f64) -> f64 {
    stagger_secs / (k_aware + 1.0)
}

/// Bulk regime — fraction of network covered, valid for ~`0.01 ≤ f ≤
/// 0.95` on sparse graphs.
///
/// Derivation: treat propagation as an SI epidemic on the peer graph
/// with **pull rate** `β = d_mean / stagger` (each non-aware node's
/// `k_aware` peers fire tickers at total rate `k_aware / stagger`; in
/// mean-field, `k_aware ≈ d_mean · f`, so the per-non-aware rate is
/// `d_mean · f / stagger`). The deterministic SI ODE is:
///
/// ```text
///   df/dt = (1 - f) · β · f
///         = (1 - f) · (d_mean / stagger) · f
/// ```
///
/// with initial condition `f(0) = 1/n`. Separation of variables gives
/// the logistic solution; inverting yields:
///
/// ```text
///   t(f) = (stagger / d_mean) · ln( (n - 1) · f / (1 - f) )
/// ```
///
/// The `(n - 1) · f / (1 - f)` term is the **odds-ratio**: how much
/// more probable it is to be aware than not, scaled by the population
/// size.
pub fn coverage_bulk(f: f64, stagger_secs: f64, d_mean: f64, n: usize) -> f64 {
    let odds = (n as f64 - 1.0) * f / (1.0 - f).max(1e-12);
    (stagger_secs / d_mean) * odds.ln()
}

/// Tail regime — extra time to cover the periphery beyond `f ≈ 0.95`.
///
/// Derivation: nodes at the **diameter periphery** are typically
/// reached via a single path with `k_aware ≈ 1` per remaining hop
/// (mean-field saturation doesn't help them — they have no shortcut
/// neighbours). The number of extra hops beyond the typical path is
/// roughly `(diameter - mean_path_length)`; per-hop time at the
/// periphery is the **single-source** value
/// `per_hop_seconds(stagger, 1) = stagger / 2`. Empirically `stagger /
/// 3` is a better fit (the periphery isn't *exactly* single-source —
/// some periphery nodes still have a couple of aware peers by the
/// time the bulk has saturated). The `/3` divisor is the empirical
/// correction; `/2` is the strict order-statistics upper bound.
pub fn coverage_tail(stagger_secs: f64, diameter: f64, mean_path_length: f64) -> f64 {
    (diameter - mean_path_length).max(0.0) * stagger_secs / 3.0
}

/// Combined coverage-time predictor.
///
/// For `f < 0.999`: bulk regime only, with [`SPARSE_FUDGE`] correction.
/// For `f >= 0.999`: bulk(0.95) + tail term.
///
/// Returns seconds. Output is a best-effort estimate; expect ±15%
/// error vs sim on real topologies.
pub fn predict_coverage(
    f: f64,
    stagger_secs: f64,
    d_mean: f64,
    n: usize,
    diameter: f64,
    mean_path_length: f64,
) -> f64 {
    // Bulk has a log-singularity at f=1. Cap at f=0.99 so the bulk
    // component stays bounded; everything past that is handled by the
    // periphery tail term. Without this, p100 explodes because the
    // log term doubles between 0.99 and 0.999.
    let f_eval = f.min(0.99);
    // Fudge applies to bulk only — the periphery term already
    // captures graph-structure penalty via (D - L̄), so applying the
    // mean-field correction on top would double-count.
    if f >= 0.999 {
        coverage_bulk(f_eval, stagger_secs, d_mean, n)
            + coverage_tail(stagger_secs, diameter, mean_path_length)
    } else {
        SPARSE_FUDGE * coverage_bulk(f_eval, stagger_secs, d_mean, n)
    }
}

/// Tiers matched to the CLI's `COVERAGE_TIERS`; kept here so the
/// predicted-vs-observed comparison stays in sync.
pub const COVERAGE_TIERS: &[f64] = &[0.05, 0.25, 0.50, 0.75, 0.95, 0.99, 1.00];

/// Format the predictor's output for one parameter set. Used by
/// `sim::run` to print a table after the topology summary; the table
/// rows mirror the CLI's later per-tier observed-time table.
pub fn render_prediction_table(
    stagger_secs: f64,
    d_mean: f64,
    n: usize,
    diameter: f64,
    mean_path_length: f64,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "predicted sketch coverage times (SI-model, σ={stagger_secs:.1}s, \
         d̄={d_mean:.2}, n={n}, D={diameter:.0}, L̄={mean_path_length:.2}):\n"
    ));
    for &f in COVERAGE_TIERS {
        let t = predict_coverage(f, stagger_secs, d_mean, n, diameter, mean_path_length);
        out.push_str(&format!(
            "  p{:>3.0}: {:>7.1}s\n",
            f * 100.0,
            t,
        ));
    }
    out
}
