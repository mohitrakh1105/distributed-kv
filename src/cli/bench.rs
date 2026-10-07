//! `dkv bench`: a closed-loop load generator with just enough statistics to
//! teach something.
//!
//! Closed-loop means each writer waits for its commit before sending the next
//! write, so the numbers are commit latency as a client experiences it. The
//! interesting comparison is not the absolute figures — a laptop loopback says
//! nothing about production — it is how they move when you change one thing:
//! `--fsync never` vs `always`, or a follower killed mid-run.

use super::commands::{Cluster, fmt_hash, new_client_id};
use crate::proto::{Op, Reply};
use std::time::{Duration, Instant};

pub struct BenchResult {
    pub ops: usize,
    pub errors: usize,
    pub elapsed: Duration,
    pub latencies_ms: Vec<f64>,
    pub log_growth: u64,
    pub leader: Option<u8>,
}

impl BenchResult {
    pub fn throughput(&self) -> f64 {
        if self.elapsed.is_zero() {
            return 0.0;
        }
        (self.ops - self.errors) as f64 / self.elapsed.as_secs_f64()
    }

    pub fn percentile(&self, p: f64) -> f64 {
        if self.latencies_ms.is_empty() {
            return 0.0;
        }
        let mut sorted = self.latencies_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let rank = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted[rank.min(sorted.len() - 1)]
    }
}

pub async fn run_once(
    cluster: &Cluster,
    ops: usize,
    conns: usize,
    value_bytes: usize,
) -> BenchResult {
    let conns = conns.max(1);
    let per_conn = ops.div_ceil(conns);
    let before = cluster.status_all().await;
    let last_index_before = before
        .iter()
        .filter_map(|(_, s)| s.as_ref())
        .map(|s| s.last_index)
        .max()
        .unwrap_or(0);
    // Highest term wins: during a partition more than one node claims the role.
    let leader = before
        .iter()
        .filter_map(|(_, s)| s.as_ref())
        .filter(|s| s.role == "LEADER")
        .max_by_key(|s| s.term)
        .map(|s| s.id);

    let value = "x".repeat(value_bytes.max(1));
    let started = Instant::now();
    let mut handles = Vec::new();
    for c in 0..conns {
        let cluster = *cluster;
        let value = value.clone();
        handles.push(tokio::spawn(async move {
            let client_id = new_client_id();
            let mut latencies = Vec::with_capacity(per_conn);
            let mut errors = 0usize;
            for i in 0..per_conn {
                let key = format!("bench:{c}:{i}");
                let t = Instant::now();
                let result = cluster
                    .send_op(
                        leader,
                        client_id,
                        (i + 1) as u64,
                        Op::Set {
                            key,
                            value: value.clone(),
                        },
                    )
                    .await;
                match result {
                    Ok(outcome) if matches!(outcome.reply, Reply::Committed { .. }) => {
                        latencies.push(t.elapsed().as_secs_f64() * 1000.0);
                    }
                    _ => errors += 1,
                }
            }
            (latencies, errors)
        }));
    }

    let mut latencies_ms = Vec::with_capacity(ops);
    let mut errors = 0;
    for h in handles {
        if let Ok((l, e)) = h.await {
            latencies_ms.extend(l);
            errors += e;
        }
    }
    let elapsed = started.elapsed();

    let after = cluster.status_all().await;
    let last_index_after = after
        .iter()
        .filter_map(|(_, s)| s.as_ref())
        .map(|s| s.last_index)
        .max()
        .unwrap_or(0);

    BenchResult {
        ops: per_conn * conns,
        errors,
        elapsed,
        latencies_ms,
        log_growth: last_index_after.saturating_sub(last_index_before),
        leader,
    }
}

pub fn print_header(cluster: &Cluster) {
    println!(
        "{}",
        cluster
            .dim("label            ops   errors   ops/s    p50ms   p95ms   p99ms   maxms   log+")
    );
}

pub fn print_row(cluster: &Cluster, label: &str, r: &BenchResult) {
    let max = r.latencies_ms.iter().cloned().fold(0.0_f64, f64::max);
    let errors = if r.errors > 0 {
        cluster.bad(&format!("{:>6}", r.errors))
    } else {
        format!("{:>6}", r.errors)
    };
    println!(
        "{:<14} {:>6}   {}  {:>7.0}  {:>7.1} {:>7.1} {:>7.1} {:>7.1}  {:>5}",
        label,
        r.ops,
        errors,
        r.throughput(),
        r.percentile(50.0),
        r.percentile(95.0),
        r.percentile(99.0),
        max,
        r.log_growth
    );
}

pub async fn run(cluster: Cluster, ops: usize, conns: usize, value_bytes: usize) {
    let statuses = cluster.status_all().await;
    let fsync = statuses
        .iter()
        .filter_map(|(_, s)| s.as_ref())
        .map(|s| s.fsync.clone())
        .next()
        .unwrap_or_else(|| "?".into());
    println!(
        "{}",
        cluster.bold(&format!(
            "bench: {ops} writes over {conns} connection(s), {value_bytes}-byte values, fsync={fsync}"
        ))
    );
    let result = run_once(&cluster, ops, conns, value_bytes).await;
    print_header(&cluster);
    print_row(&cluster, &format!("fsync={fsync}"), &result);
    if let Some(l) = result.leader {
        println!(
            "{}",
            cluster.dim(&format!("leader was n{l}; every write went through it"))
        );
    }
    // Followers are still applying the last entries when the final write
    // returns, so sampling immediately prints three different hashes and makes a
    // healthy cluster look broken. Let it settle first.
    let deadline = Instant::now() + Duration::from_secs(5);
    let after = loop {
        let statuses = cluster.status_all().await;
        let up: Vec<_> = statuses.iter().filter_map(|(_, s)| s.as_ref()).collect();
        let settled = up.len() == statuses.len()
            && up
                .iter()
                .all(|s| s.state_hash == up[0].state_hash && s.last_applied == up[0].last_applied);
        if settled || Instant::now() >= deadline {
            break statuses;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    };
    let hashes: Vec<String> = after
        .iter()
        .filter_map(|(id, s)| {
            s.as_ref()
                .map(|s| format!("n{id}={}", fmt_hash(s.state_hash)))
        })
        .collect();
    println!(
        "{}",
        cluster.dim(&format!("state hashes once applied: {}", hashes.join(" ")))
    );
    println!();
    println!("{}", cluster.dim("Things to try, then run this again:"));
    println!(
        "{}",
        cluster
            .dim("  dkv down; dkv up --fsync never      the cost of durability, in milliseconds")
    );
    println!("{}", cluster.dim("  dkv kill <follower>                  quorum is 2 of 3 — a dead follower costs nothing"));
    println!(
        "{}",
        cluster.dim(
            "  dkv fault <follower> --delay 200     commit waits for the majority, not the slowest"
        )
    );
    println!(
        "{}",
        cluster
            .dim("  dkv inspect snap                     watch the log+ column drive compaction")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(latencies: Vec<f64>) -> BenchResult {
        BenchResult {
            ops: latencies.len(),
            errors: 0,
            elapsed: Duration::from_secs(1),
            latencies_ms: latencies,
            log_growth: 0,
            leader: None,
        }
    }

    #[test]
    fn percentiles_come_from_the_sorted_latencies_not_arrival_order() {
        let r = result(vec![50.0, 10.0, 30.0, 20.0, 40.0]);
        assert_eq!(r.percentile(50.0), 30.0);
        assert_eq!(r.percentile(100.0), 50.0);
        assert_eq!(r.percentile(0.0), 10.0);
    }

    #[test]
    fn an_empty_run_reports_zero_rather_than_panicking() {
        let r = result(vec![]);
        assert_eq!(r.percentile(99.0), 0.0);
        assert_eq!(r.throughput(), 0.0);
    }
}
