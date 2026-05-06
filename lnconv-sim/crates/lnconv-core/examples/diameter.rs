//! Print BFS-derived diameter and mean path length for the random k-regular
//! generator across a range of (n, k). Useful for sanity-checking
//! propagation expectations before running a full sim.
use lnconv_core::topology::{metrics, synthetic};

fn main() {
    let cases: &[(usize, usize)] = &[
        (20, 2),
        (200, 8),
        (1000, 8),
        (1000, 16),
        (20000, 6),
        (20000, 16),
    ];
    let seed = 1;

    println!(
        "{:>8} {:>4}   {:>9} {:>9} {:>10} {:>9}",
        "n", "k", "diameter", "mean_pl", "edges", "exact"
    );
    for &(n, k) in cases {
        let g = synthetic::random_regular(n, k, seed);
        let m = metrics::compute(&g, seed, 2000, 1000);
        println!(
            "{:>8} {:>4}   {:>9} {:>9.2} {:>10} {:>9}",
            n, k, m.diameter, m.mean_path_length, m.edges, m.exact
        );
    }
}
