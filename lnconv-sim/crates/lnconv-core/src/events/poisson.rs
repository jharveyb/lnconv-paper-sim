//! Rate-based event generator. Approximates a network-wide steady-state
//! gossip load — e.g. "the LN as a whole produces ~7 channel updates per
//! second from random nodes, indefinitely".

use std::time::Duration;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use statrs::distribution::{ContinuousCDF, Exp};

use super::EventSchedule;
use crate::channels::ChannelRegistry;
use crate::message::{Gossip, GossipKind, NodeId};

/// Poisson-process stream: messages arrive with exponential inter-arrival
/// times of mean `1 / rate_per_sec`. For each event we sample a
/// `(scid, direction)` uniformly from the channel registry and look up
/// the owner — that node is the originator. Stream stops when its
/// accumulated time exceeds the simulation's `duration_seconds`. Sampled
/// deterministically from `seed` so reruns reproduce.
///
/// Inter-arrivals are sampled by feeding a uniform `[0, 1)` from our
/// `ChaCha8Rng` through `statrs::distribution::Exp::inverse_cdf`. The
/// inverse-CDF route is needed because statrs's `Distribution::sample`
/// targets a different `rand` major version than the rest of this
/// workspace; it leaves statrs in charge of the actual distribution
/// math while keeping a single `rand` ecosystem.
pub struct PoissonRandom {
    pub rate_per_sec: f64,
    pub seed: u64,
    pub size_bytes: u16,
}

impl EventSchedule for PoissonRandom {
    fn build(
        &self,
        nodes: &[NodeId],
        max: Duration,
        registry: &ChannelRegistry,
    ) -> Vec<(Duration, NodeId, Gossip)> {
        if self.rate_per_sec <= 0.0 || nodes.is_empty() || registry.is_empty() {
            return Vec::new();
        }
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);
        let exp = Exp::new(self.rate_per_sec).expect("rate must be positive");
        let max_secs = max.as_secs_f64();
        let mut t_secs = 0.0_f64;
        let mut events = Vec::new();
        loop {
            // Strict (0, 1) so inverse_cdf never sees the boundary value.
            let u = rng.random::<f64>().clamp(f64::EPSILON, 1.0 - f64::EPSILON);
            let dt = exp.inverse_cdf(u);
            t_secs += dt;
            if t_secs > max_secs {
                break;
            }
            let (scid, direction, origin) = registry.random_channel(&mut rng);
            events.push((
                Duration::from_secs_f64(t_secs),
                origin,
                Gossip {
                    id: 0, // originate's stamp re-derives this
                    origin: None, // ChannelUpdate has no wire origin
                    kind: GossipKind::ChannelUpdate,
                    size_bytes: self.size_bytes,
                    scid: Some(scid),
                    direction,
                    timestamp: 0,
                },
            ));
        }
        events
    }
}
