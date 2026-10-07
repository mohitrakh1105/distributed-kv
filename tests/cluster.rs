//! Integration tests: a real three-node cluster, as three real processes,
//! driven the way the CLI drives it.
//!
//! These use processes rather than in-process nodes deliberately. The behaviours
//! worth testing here — a hard kill losing an unflushed tail, a log replayed
//! from a file on restart, a partition that leaves the sockets open — only exist
//! because there are separate processes and separate files. An in-process
//! harness would be faster and would quietly test something else.
//!
//! Each test gets its own port base and data directory so they can run
//! concurrently, which is how `cargo test` will run them.

use dkv::cli::commands::Cluster;
use dkv::control::Control;
use dkv::proto::{AdminCmd, AdminReply, NodeStatus, Op, Reply};
use dkv::types::NodeId;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

/// Each test claims a distinct block of four ports.
static NEXT_PORT_BASE: AtomicU16 = AtomicU16::new(6400);

struct TestCluster {
    port_base: u16,
    data_dir: PathBuf,
    children: Vec<(NodeId, Child)>,
    fsync: String,
}

impl TestCluster {
    fn start(name: &str, fsync: &str) -> TestCluster {
        let port_base = NEXT_PORT_BASE.fetch_add(10, Ordering::SeqCst);
        let data_dir = std::env::temp_dir().join(format!("dkv-it-{name}-{port_base}"));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("data dir");

        let mut cluster = TestCluster {
            port_base,
            data_dir,
            children: Vec::new(),
            fsync: fsync.to_string(),
        };
        for id in [1, 2, 3] {
            cluster.spawn(id);
        }
        cluster
    }

    fn spawn(&mut self, id: NodeId) {
        let exe = node_binary();
        let child = Command::new(exe)
            .args([
                "--id",
                &id.to_string(),
                "--port-base",
                &self.port_base.to_string(),
                "--data",
                self.data_dir.to_str().expect("utf8 path"),
                "--fsync",
                &self.fsync,
                // Faster than the interactive defaults — nothing here is being
                // watched by a human — but not so fast that a loaded machine
                // running several of these suites at once starts timing out
                // healthy leaders and failing for no reason.
                "--heartbeat",
                "120",
                "--election-min",
                "500",
                "--election-max",
                "1000",
                "--snapshot-threshold",
                "40",
                "--no-color",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null())
            .spawn()
            .expect("spawn node");
        self.children.push((id, child));
    }

    fn handle(&self) -> Cluster {
        Cluster {
            port_base: self.port_base,
            color: false,
        }
    }

    /// TerminateProcess: no unwind, no flush. The same thing `dkv kill` does.
    fn kill(&mut self, id: NodeId) {
        if let Some(pos) = self.children.iter().position(|(n, _)| *n == id) {
            let (_, mut child) = self.children.remove(pos);
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for (_, child) in self.children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn node_binary() -> PathBuf {
    // `cargo test` puts the integration test binary in target/<profile>/deps,
    // so the node binary is two levels up.
    let mut path = std::env::current_exe().expect("test exe path");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join(format!("dkv-node{}", std::env::consts::EXE_SUFFIX))
}

async fn wait_for_leader(cluster: &Cluster) -> NodeId {
    cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("a leader should be elected within 15s")
}

async fn set(cluster: &Cluster, node: Option<NodeId>, key: &str, value: &str) -> Reply {
    cluster
        .send_op(
            node,
            dkv::cli::commands::new_client_id(),
            1,
            Op::Set {
                key: key.into(),
                value: value.into(),
            },
        )
        .await
        .map(|o| o.reply)
        .unwrap_or_else(|e| Reply::Error {
            kind: dkv::proto::ErrorKind::Internal,
            message: format!("{e}"),
            leader_hint: None,
        })
}

async fn get(cluster: &Cluster, node: Option<NodeId>, key: &str, stale: bool) -> Reply {
    cluster
        .send_op(
            node,
            dkv::cli::commands::new_client_id(),
            1,
            Op::Get {
                key: key.into(),
                stale,
            },
        )
        .await
        .map(|o| o.reply)
        .unwrap_or_else(|e| Reply::Error {
            kind: dkv::proto::ErrorKind::Internal,
            message: format!("{e}"),
            leader_hint: None,
        })
}

/// Waits until every node that answers reports the same applied index and state
/// hash. Returns false if that never happens.
async fn converges(cluster: &Cluster, expect_nodes: usize, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let statuses = cluster.status_all().await;
        let up: Vec<NodeStatus> = statuses.into_iter().filter_map(|(_, s)| s).collect();
        if up.len() == expect_nodes
            && up
                .iter()
                .all(|s| s.state_hash == up[0].state_hash && s.last_applied == up[0].last_applied)
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    false
}

async fn cut(cluster: &Cluster, a: NodeId, b: NodeId, on: bool) {
    let _ = cluster.admin(a, AdminCmd::Partition { peer: b, on }).await;
    let _ = cluster.admin(b, AdminCmd::Partition { peer: a, on }).await;
}

async fn isolate(cluster: &Cluster, node: NodeId) {
    for peer in [1, 2, 3] {
        if peer != node {
            cut(cluster, node, peer, true).await;
        }
    }
}

async fn heal_all(cluster: &Cluster) {
    for a in [1u8, 2, 3] {
        for b in [1u8, 2, 3] {
            if a < b {
                cut(cluster, a, b, false).await;
            }
        }
    }
}

async fn log_of(cluster: &Cluster, node: NodeId) -> (Vec<dkv::types::LogEntry>, u64) {
    match cluster
        .admin(
            node,
            AdminCmd::InspectLog {
                from: None,
                to: None,
            },
        )
        .await
    {
        Ok(AdminReply::LogEntries {
            entries,
            commit_index,
            ..
        }) => (entries, commit_index),
        _ => (Vec::new(), 0),
    }
}

#[tokio::test]
async fn a_fresh_cluster_elects_exactly_one_leader_and_replicates_a_write() {
    let cluster = TestCluster::start("basic", "always");
    let c = cluster.handle();
    let leader = wait_for_leader(&c).await;

    let reply = set(&c, None, "name", "mohit").await;
    assert!(
        matches!(reply, Reply::Committed { .. }),
        "a write to a healthy cluster must commit, got {reply:?}"
    );

    assert!(converges(&c, 3, Duration::from_secs(10)).await);

    let statuses = c.status_all().await;
    let leaders: Vec<NodeId> = statuses
        .iter()
        .filter_map(|(_, s)| s.as_ref())
        .filter(|s| s.role == "LEADER")
        .map(|s| s.id)
        .collect();
    assert_eq!(
        leaders,
        vec![leader],
        "exactly one node may claim leadership"
    );

    // Every node applied it, not just the leader.
    for node in [1, 2, 3] {
        match get(&c, Some(node), "name", true).await {
            Reply::Value { value, .. } => assert_eq!(value, Some("mohit".to_string())),
            other => panic!("n{node} should hold the value, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn killing_the_leader_elects_a_new_one_and_writes_keep_working() {
    let mut cluster = TestCluster::start("failover", "always");
    let c = cluster.handle();
    let first = wait_for_leader(&c).await;
    set(&c, None, "before", "yes").await;

    cluster.kill(first);

    let second = c
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("the survivors must elect a new leader");
    assert_ne!(second, first, "the dead node cannot still be leader");

    let reply = set(&c, None, "after", "yes").await;
    assert!(
        matches!(reply, Reply::Committed { .. }),
        "two of three nodes is a majority, so writes must still commit"
    );

    cluster.spawn(first);
    assert!(
        converges(&c, 3, Duration::from_secs(15)).await,
        "the restarted node must catch up to the others"
    );
}

#[tokio::test]
async fn a_leader_in_the_minority_cannot_commit_and_its_entry_is_truncated_on_heal() {
    let cluster = TestCluster::start("partition", "always");
    let c = cluster.handle();
    let old_leader = wait_for_leader(&c).await;
    set(&c, None, "a", "1").await;

    isolate(&c, old_leader).await;

    // The isolated leader accepts the request into its log, but no majority can
    // ack it, so the client must NOT be told it succeeded.
    let reply = set(&c, Some(old_leader), "ghost", "boo").await;
    assert!(
        !matches!(reply, Reply::Committed { .. }),
        "a minority leader must never report a write as committed, got {reply:?}"
    );

    // It did append it though — that is exactly the entry that is about to be lost.
    let (entries, commit) = log_of(&c, old_leader).await;
    let ghost = entries
        .iter()
        .find(|e| e.command.summary() == "SET ghost=boo")
        .expect("the isolated leader appended the entry");
    assert!(
        ghost.index > commit,
        "the entry must be sitting uncommitted, not committed"
    );

    // The majority side elects someone else and carries on.
    let deadline = Instant::now() + Duration::from_secs(15);
    let new_leader = loop {
        if let Some(l) = c.leader().await
            && l != old_leader
        {
            break l;
        }
        assert!(
            Instant::now() < deadline,
            "the majority must elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let reply = set(&c, Some(new_leader), "real", "yes").await;
    assert!(
        matches!(reply, Reply::Committed { .. }),
        "the majority side must still be able to commit"
    );

    heal_all(&c).await;
    assert!(
        converges(&c, 3, Duration::from_secs(20)).await,
        "the cluster must reconcile after the partition heals"
    );

    // The lost write is genuinely gone, everywhere.
    for node in [1, 2, 3] {
        match get(&c, Some(node), "ghost", true).await {
            Reply::Value { value, .. } => assert_eq!(
                value, None,
                "n{node} still has a write that was never committed"
            ),
            other => panic!("unexpected reply from n{node}: {other:?}"),
        }
    }
    match get(&c, None, "real", false).await {
        Reply::Value { value, .. } => assert_eq!(value, Some("yes".to_string())),
        other => panic!("the committed write must survive, got {other:?}"),
    }
}

#[tokio::test]
async fn a_committed_value_never_changes_even_across_partitions_and_elections() {
    // The Figure 8 property, checked the only way that means anything: watch a
    // committed index continuously and assert its value never moves.
    let cluster = TestCluster::start("figure8", "always");
    let c = cluster.handle();
    wait_for_leader(&c).await;

    set(&c, None, "stable", "original").await;
    let committed_at = match get(&c, None, "stable", false).await {
        Reply::Value { index, .. } => index,
        other => panic!("expected a value, got {other:?}"),
    };

    for round in 0..3 {
        let leader = c.leader().await.unwrap_or(1);
        isolate(&c, leader).await;
        let _ = set(&c, Some(leader), &format!("doomed{round}"), "x").await;
        tokio::time::sleep(Duration::from_millis(900)).await;
        heal_all(&c).await;
        assert!(
            converges(&c, 3, Duration::from_secs(20)).await,
            "round {round} failed to converge"
        );

        // The committed entry must still be the same entry, with the same value.
        for node in [1, 2, 3] {
            match get(&c, Some(node), "stable", true).await {
                Reply::Value { value, .. } => assert_eq!(
                    value,
                    Some("original".to_string()),
                    "n{node} changed a committed value in round {round}"
                ),
                other => panic!("unexpected reply: {other:?}"),
            }
            let (entries, _) = log_of(&c, node).await;
            if let Some(e) = entries.iter().find(|e| e.index == committed_at) {
                assert_eq!(
                    e.command.summary(),
                    "SET stable=original",
                    "n{node} put a different command at a committed index"
                );
            }
        }
    }
}

#[tokio::test]
async fn a_hard_kill_loses_nothing_that_was_acknowledged() {
    let mut cluster = TestCluster::start("durability", "always");
    let c = cluster.handle();
    wait_for_leader(&c).await;

    // Write, and remember only what was actually confirmed committed.
    let mut confirmed = Vec::new();
    for i in 0..12 {
        let key = format!("k{i}");
        if matches!(
            set(&c, None, &key, &format!("v{i}")).await,
            Reply::Committed { .. }
        ) {
            confirmed.push(key);
        }
    }
    assert!(confirmed.len() >= 10, "most writes should have committed");

    // Kill every node at once, hard. Nothing gets to flush or tidy up.
    for id in [1, 2, 3] {
        cluster.kill(id);
    }
    for id in [1, 2, 3] {
        cluster.spawn(id);
    }
    wait_for_leader(&c).await;
    assert!(converges(&c, 3, Duration::from_secs(20)).await);

    for key in &confirmed {
        match get(&c, None, key, false).await {
            Reply::Value { value, .. } => assert!(
                value.is_some(),
                "{key} was acknowledged as committed and then lost across a restart"
            ),
            other => panic!("unexpected reply for {key}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_node_that_missed_a_compaction_is_caught_up_with_a_snapshot() {
    let mut cluster = TestCluster::start("snapshot", "always");
    let c = cluster.handle();
    let leader = wait_for_leader(&c).await;
    let victim = [1u8, 2, 3]
        .into_iter()
        .find(|n| *n != leader)
        .expect("a follower exists");

    cluster.kill(victim);

    // Well past the 40-entry snapshot threshold these tests start nodes with.
    for i in 0..60 {
        set(&c, Some(leader), &format!("k{i}"), &format!("v{i}")).await;
    }

    // The leader should have compacted by now, so the entries the dead follower
    // needs no longer exist as log entries anywhere.
    let snap = match c.admin(leader, AdminCmd::InspectSnapshot).await {
        Ok(AdminReply::Snapshot {
            last_included_index,
            ..
        }) => last_included_index,
        other => panic!("expected snapshot info, got {other:?}"),
    };
    assert!(snap > 0, "the leader should have taken a snapshot by now");

    cluster.spawn(victim);
    assert!(
        converges(&c, 3, Duration::from_secs(25)).await,
        "the follower must be caught up via InstallSnapshot"
    );

    match get(&c, Some(victim), "k59", true).await {
        Reply::Value { value, .. } => assert_eq!(value, Some("v59".to_string())),
        other => panic!("the restored node is missing data: {other:?}"),
    }
}

#[tokio::test]
async fn the_same_request_sent_twice_is_applied_once() {
    let cluster = TestCluster::start("dedup", "always");
    let c = cluster.handle();
    wait_for_leader(&c).await;

    let client_id = 4242;
    let first = c
        .send_op(
            None,
            client_id,
            7,
            Op::Set {
                key: "once".into(),
                value: "first".into(),
            },
        )
        .await
        .expect("first write");
    assert!(matches!(
        first.reply,
        Reply::Committed {
            duplicate: false,
            ..
        }
    ));

    // A different value under the same (client, seq): a retry of the original
    // request, as far as the cluster is concerned.
    let retry = c
        .send_op(
            None,
            client_id,
            7,
            Op::Set {
                key: "once".into(),
                value: "second".into(),
            },
        )
        .await
        .expect("retry");
    match retry.reply {
        Reply::Committed { duplicate, .. } => {
            assert!(duplicate, "the retry must be recognised as one")
        }
        other => panic!("unexpected reply: {other:?}"),
    }

    match get(&c, None, "once", false).await {
        Reply::Value { value, .. } => assert_eq!(
            value,
            Some("first".to_string()),
            "the retry must not have overwritten anything"
        ),
        other => panic!("unexpected reply: {other:?}"),
    }
}

#[tokio::test]
async fn a_linearizable_read_refuses_where_a_stale_read_answers_confidently() {
    let cluster = TestCluster::start("reads", "always");
    let c = cluster.handle();
    let old_leader = wait_for_leader(&c).await;
    set(&c, None, "k", "before").await;

    isolate(&c, old_leader).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    let new_leader = loop {
        if let Some(l) = c.leader().await
            && l != old_leader
        {
            break l;
        }
        assert!(Instant::now() < deadline, "majority must elect a leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    set(&c, Some(new_leader), "k", "after").await;

    // The isolated node still believes it is leader and will happily serve its
    // own out-of-date state.
    match get(&c, Some(old_leader), "k", true).await {
        Reply::Value { value, stale, .. } => {
            assert_eq!(value, Some("before".to_string()));
            assert!(stale);
        }
        other => panic!("a stale read should have answered, got {other:?}"),
    }

    // The same node, asked for a linearizable read, must refuse.
    match get(&c, Some(old_leader), "k", false).await {
        Reply::Error { kind, .. } => assert!(
            matches!(
                kind,
                dkv::proto::ErrorKind::ReadTimeout | dkv::proto::ErrorKind::NotLeader
            ),
            "expected a refusal, got {kind:?}"
        ),
        other => panic!("a linearizable read must not be answered here, got {other:?}"),
    }

    heal_all(&c).await;
    assert!(converges(&c, 3, Duration::from_secs(20)).await);
}

#[tokio::test]
async fn heavy_packet_loss_costs_progress_but_never_consistency() {
    let cluster = TestCluster::start("flaky", "always");
    let c = cluster.handle();
    wait_for_leader(&c).await;

    for node in [1, 2, 3] {
        let _ = c
            .admin(
                node,
                AdminCmd::Fault {
                    peer: None,
                    drop_rate: Some(0.3),
                    delay_ms: Some(30),
                    clear: false,
                },
            )
            .await;
    }

    let mut committed = 0;
    for i in 0..15 {
        if matches!(
            set(&c, None, &format!("f{i}"), "v").await,
            Reply::Committed { .. }
        ) {
            committed += 1;
        }
    }
    assert!(
        committed > 0,
        "retries should get at least some writes through 30% loss"
    );

    for node in [1, 2, 3] {
        let _ = c
            .admin(
                node,
                AdminCmd::Fault {
                    peer: None,
                    drop_rate: None,
                    delay_ms: None,
                    clear: true,
                },
            )
            .await;
    }
    assert!(
        converges(&c, 3, Duration::from_secs(25)).await,
        "a lossy network may cost time, never agreement"
    );
}

#[tokio::test]
async fn the_supervisor_kills_its_children_when_it_dies() {
    // The deadman socket: no process groups, no FFI, just a connection whose
    // close means "your parent is gone".
    let port_base = NEXT_PORT_BASE.fetch_add(10, Ordering::SeqCst);
    let data_dir = std::env::temp_dir().join(format!("dkv-it-deadman-{port_base}"));
    let _ = std::fs::remove_dir_all(&data_dir);

    let mut dkv_exe = node_binary();
    dkv_exe.set_file_name(format!("dkv{}", std::env::consts::EXE_SUFFIX));
    let mut supervisor = Command::new(&dkv_exe)
        .args([
            "--port-base",
            &port_base.to_string(),
            "--no-color",
            "up",
            "--fresh",
            "--data",
            data_dir.to_str().expect("utf8 path"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .spawn()
        .expect("spawn supervisor");

    let c = Cluster {
        port_base,
        color: false,
    };
    let leader = c.wait_for_leader(Duration::from_secs(15)).await;
    assert!(leader.is_some(), "the supervised cluster should come up");

    // Kill the supervisor without letting it shut anything down.
    let _ = supervisor.kill();
    let _ = supervisor.wait();

    // The children should notice their line went dead and exit on their own.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let answering = c
            .status_all()
            .await
            .into_iter()
            .filter(|(_, s)| s.is_some())
            .count();
        if answering == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{answering} node(s) outlived the supervisor — they are orphans"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn the_control_port_is_how_a_second_terminal_finds_a_running_cluster() {
    let port_base = NEXT_PORT_BASE.fetch_add(10, Ordering::SeqCst);
    // Nothing is listening yet.
    assert!(!dkv::cli::supervise::supervisor_is_running(port_base).await);
    assert!(
        dkv::cli::supervise::control(port_base, Control::Running)
            .await
            .is_err(),
        "with no supervisor, a control command must fail rather than hang"
    );
}
