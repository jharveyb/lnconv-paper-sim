//! Print BFS-derived diameter and mean path length for the random k-regular
//! generator across a range of (n, k). Useful for sanity-checking
//! propagation expectations before running a full sim.
use lnconv_core::topology::{NodeAlgo, metrics, synthetic};

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
        // Vertex algo doesn't matter for the BFS — pass anything.
        let g = synthetic::random_regular(n, k, seed, NodeAlgo::Flooding);
        let m = metrics::compute(&g, seed, 2000, 1000);
        // Synthetic random_regular has no channels yet, so only peers
        // matters; print its block.
        let p = &m.peers;
        println!(
            "{:>8} {:>4}   {:>9} {:>9.2} {:>10} {:>9}",
            n, k, p.diameter, p.mean_path_length, p.edges, p.exact
        );
    }
}
