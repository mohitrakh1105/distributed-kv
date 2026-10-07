//! Fake network weather, per link, switchable at runtime.
//!
//! A partition here does **not** close the TCP connection: the process stays up,
//! keeps its beliefs, and simply stops being heard. That gap between "I am the
//! leader" and "anyone can hear me" is the thing worth watching, and dropping
//! the socket instead would give it away immediately.

use crate::proto::FaultStatus;
use crate::types::NodeId;
use rand::Rng;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LinkFault {
    pub partitioned: bool,
    pub drop_rate: f64,
    pub delay_ms: u64,
}

impl LinkFault {
    fn is_clean(&self) -> bool {
        !self.partitioned && self.drop_rate <= 0.0 && self.delay_ms == 0
    }
}

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Deliver { delay: Duration },
    Dropped { reason: &'static str },
}

#[derive(Debug, Default)]
pub struct Faults {
    links: HashMap<NodeId, LinkFault>,
}

impl Faults {
    pub fn new() -> Self {
        Faults::default()
    }

    /// Consulted on both send and receive. Applying it twice is deliberate: a
    /// one-sided `part 1 3` still behaves like a real partition, because node 1
    /// refuses to hear node 3 even if node 3 never learned about the split.
    pub fn decide(&self, peer: NodeId) -> Verdict {
        let Some(fault) = self.links.get(&peer) else {
            return Verdict::Deliver {
                delay: Duration::ZERO,
            };
        };
        if fault.partitioned {
            return Verdict::Dropped {
                reason: "partitioned",
            };
        }
        if fault.drop_rate > 0.0 && rand::rng().random::<f64>() < fault.drop_rate {
            return Verdict::Dropped { reason: "dropped" };
        }
        Verdict::Deliver {
            delay: Duration::from_millis(fault.delay_ms),
        }
    }

    pub fn partition(&mut self, peer: NodeId, on: bool) {
        let entry = self.links.entry(peer).or_default();
        entry.partitioned = on;
        self.prune(peer);
    }

    pub fn set(&mut self, peer: NodeId, drop_rate: Option<f64>, delay_ms: Option<u64>) {
        let entry = self.links.entry(peer).or_default();
        if let Some(rate) = drop_rate {
            entry.drop_rate = rate.clamp(0.0, 1.0);
        }
        if let Some(ms) = delay_ms {
            entry.delay_ms = ms;
        }
        self.prune(peer);
    }

    pub fn clear_peer(&mut self, peer: NodeId) {
        self.links.remove(&peer);
    }

    pub fn clear(&mut self) {
        self.links.clear();
    }

    pub fn is_partitioned(&self, peer: NodeId) -> bool {
        self.links.get(&peer).is_some_and(|f| f.partitioned)
    }

    pub fn any(&self) -> bool {
        !self.links.is_empty()
    }

    pub fn status(&self) -> Vec<FaultStatus> {
        let mut out: Vec<FaultStatus> = self
            .links
            .iter()
            .map(|(peer, f)| FaultStatus {
                peer: *peer,
                partitioned: f.partitioned,
                drop_rate: f.drop_rate,
                delay_ms: f.delay_ms,
            })
            .collect();
        out.sort_by_key(|f| f.peer);
        out
    }

    /// Keeps `status` honest: a link with nothing wrong with it should not show
    /// up in the dashboard's fault line.
    fn prune(&mut self, peer: NodeId) {
        if self.links.get(&peer).is_some_and(|f| f.is_clean()) {
            self.links.remove(&peer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_link_with_no_fault_delivers_immediately() {
        let faults = Faults::new();
        assert_eq!(
            faults.decide(2),
            Verdict::Deliver {
                delay: Duration::ZERO
            }
        );
    }

    #[test]
    fn a_partitioned_link_drops_everything() {
        let mut faults = Faults::new();
        faults.partition(3, true);
        for _ in 0..50 {
            assert!(matches!(faults.decide(3), Verdict::Dropped { .. }));
        }
        assert!(matches!(faults.decide(2), Verdict::Deliver { .. }));
    }

    #[test]
    fn healing_a_partition_removes_it_from_the_reported_faults() {
        let mut faults = Faults::new();
        faults.partition(3, true);
        assert_eq!(faults.status().len(), 1);
        faults.partition(3, false);
        assert!(faults.status().is_empty(), "a healed link is not a fault");
    }

    #[test]
    fn a_full_drop_rate_drops_and_a_zero_rate_never_does() {
        let mut faults = Faults::new();
        faults.set(2, Some(1.0), None);
        for _ in 0..50 {
            assert!(matches!(faults.decide(2), Verdict::Dropped { .. }));
        }
        faults.set(2, Some(0.0), None);
        for _ in 0..50 {
            assert!(matches!(faults.decide(2), Verdict::Deliver { .. }));
        }
    }

    #[test]
    fn delay_survives_a_later_drop_rate_change_on_the_same_link() {
        let mut faults = Faults::new();
        faults.set(2, None, Some(400));
        faults.set(2, Some(0.25), None);
        match faults.decide(2) {
            Verdict::Deliver { delay } => assert_eq!(delay, Duration::from_millis(400)),
            Verdict::Dropped { .. } => {} // 25% of the time, which is the point
        }
        let status = faults.status();
        assert_eq!(status[0].delay_ms, 400);
        assert_eq!(status[0].drop_rate, 0.25);
    }

    #[test]
    fn a_drop_rate_outside_zero_to_one_is_clamped_rather_than_trusted() {
        let mut faults = Faults::new();
        faults.set(2, Some(7.5), None);
        assert_eq!(faults.status()[0].drop_rate, 1.0);
    }
}
