//! Rate-based event generator. Approximates a network-wide steady-state
//! gossip load — e.g. "the LN as a whole produces ~7 channel updates per
//! second from random nodes, indefinitely".

use std::time::Duration;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::EventSchedule;
use crate::message::{Gossip, GossipKind, NodeId};

/// Poisson-process stream: messages arrive with exponential inter-arrival
/// times of mean `1 / rate_per_sec`, originated by a uniformly-random node
/// each time. Stream stops when its accumulated time exceeds the simulation's
/// `duration_seconds`. Sampled deterministically from `seed` so reruns
/// reproduce.
pub struct PoissonRandom {
    pub rate_per_sec: f64,
    pub seed: u64,
    pub size_bytes: u32,
}

impl EventSchedule for PoissonRandom {
    fn build(&self, num_nodes: usize, max: Duration) -> Vec<(Duration, NodeId, Gossip)> {
        if self.rate_per_sec <= 0.0 || num_nodes == 0 {
            return Vec::new();
        }
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);
        let mut t_secs = 0.0_f64;
        let max_secs = max.as_secs_f64();
        let mut events = Vec::new();
        let mut next_id: u64 = 0;
        loop {
            // Exponential inter-arrival via inverse-CDF. random::<f64>() is
            // [0, 1); we shift to (0, 1] so ln() never blows up.
            let u: f64 = 1.0 - rng.random::<f64>();
            let dt = -u.ln() / self.rate_per_sec;
            t_secs += dt;
            if t_secs > max_secs {
                break;
            }
            let origin = rng.random_range(0..num_nodes as u32);
            events.push((
                Duration::from_secs_f64(t_secs),
                origin,
                Gossip {
                    id: next_id,
                    origin,
                    kind: GossipKind::Full,
                    size_bytes: self.size_bytes,
                },
            ));
            next_id += 1;
        }
        events
    }
}
