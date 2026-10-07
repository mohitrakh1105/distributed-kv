//! Narrated demos: the flagship way to use this project.
//!
//! A demo is a `Vec<Step>` — say something, do something, explain what just
//! happened. No trait, no registry, no plugin system; adding one means adding a
//! function that returns a vector.
//!
//! The one piece of real design here is that **narration goes through the same
//! channel as the node logs**. One consumer prints everything, so a sentence
//! written before a kill always appears before the log lines that kill caused.
//! Printing narration directly from this task would race the log printer and
//! the story would arrive shuffled.

use super::commands::Cluster;
use super::supervise::{NodeOptions, Supervisor};
use crate::proto::{AdminCmd, AdminReply, Op, Reply};
use crate::types::NodeId;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

pub struct Step {
    /// Said before the action runs.
    pub say: &'static str,
    pub act: Act,
    /// Said after it, once the logs have had a moment to land. This is where the
    /// lesson goes.
    pub why: &'static str,
}

pub enum Act {
    Nothing,
    Set {
        key: &'static str,
        value: &'static str,
        node: Target,
    },
    Get {
        key: &'static str,
        node: Target,
        stale: bool,
    },
    KillLeader,
    Kill(Target),
    Start(Target),
    /// Cut the current leader off from both of its peers.
    PartitionLeader,
    Heal,
    Fault {
        drop_rate: f64,
        delay_ms: u64,
    },
    InspectLog(Target),
    Verify,
    WaitForLeader,
    /// Wait until a leader exists that is not the one we noted earlier.
    WaitForNewLeader,
    Sleep(Duration),
    Bench {
        ops: usize,
        conns: usize,
    },
    WriteBurst(usize),
}

/// Which node a step acts on. `Leader` and `Follower` resolve when the step runs
/// — a demo cannot know in advance who wins an election.
pub enum Target {
    Any,
    Node(NodeId),
    Leader,
    Follower,
    /// Whoever was leader when the demo last noted it, even if deposed since.
    OldLeader,
}

pub fn list() -> Vec<(&'static str, &'static str)> {
    vec![
        ("election", "a leader is elected, killed, and replaced"),
        ("replication", "one write, followed from client to commit"),
        ("partition", "a leader in the minority loses its writes"),
        (
            "flaky",
            "30% packet loss and 400ms delay — retries still win",
        ),
        ("durability", "hard kill with fsync off, and what survives"),
        (
            "snapshot",
            "compaction, then catching up a node in one shot",
        ),
        ("reads", "a stale read and a linearizable one, side by side"),
        ("dedup", "the same request twice, applied once"),
    ]
}

pub fn script(name: &str) -> Option<Vec<Step>> {
    match name {
        "election" => Some(election()),
        "replication" => Some(replication()),
        "partition" => Some(partition()),
        "flaky" => Some(flaky()),
        "durability" => Some(durability()),
        "snapshot" => Some(snapshot()),
        "reads" => Some(reads()),
        "dedup" => Some(dedup()),
        _ => None,
    }
}

fn election() -> Vec<Step> {
    vec![
        Step {
            say: "Three nodes just started. None of them is leader yet, and none of them\n\
                  knows anything about the others except an address. Watch them time out\n\
                  and hold an election.",
            act: Act::WaitForLeader,
            why: "One node's randomized election timer fired first, so it became a candidate,\n\
                  bumped the term, and asked the other two for votes. Two votes out of three is\n\
                  a majority, so it won. The randomization is what stops all three campaigning\n\
                  at once forever.",
        },
        Step {
            say: "Write something, so there is state worth protecting.",
            act: Act::Set {
                key: "city",
                value: "pune",
                node: Target::Any,
            },
            why: "The leader appended it, replicated it, and only answered OK once a majority\n\
                  had it on disk.",
        },
        Step {
            say: "Now kill the leader. Not a graceful shutdown — a hard kill, the way a\n\
                  power cut would do it.",
            act: Act::KillLeader,
            why: "The remaining two are still exchanging nothing but silence from the dead one.\n\
                  Nothing happens until a follower's election timer runs out.",
        },
        Step {
            say: "Wait for the survivors to notice and elect a replacement.",
            act: Act::WaitForNewLeader,
            why: "A new term, a new leader, chosen by 2 of the 3 nodes. The cluster was\n\
                  unavailable for roughly one election timeout — that is the real cost of a\n\
                  leader dying, and it is why the timeout is a tuning decision, not a constant.",
        },
        Step {
            say: "Prove the cluster still works with only two nodes alive.",
            act: Act::Set {
                key: "lang",
                value: "rust",
                node: Target::Any,
            },
            why: "Two of three is a majority, so writes commit normally. A three-node cluster\n\
                  survives exactly one failure.",
        },
        Step {
            say: "Bring the dead node back.",
            act: Act::Start(Target::OldLeader),
            why: "It comes up believing it is in the old term and was leader. The first\n\
                  AppendEntries it receives carries a higher term, and that alone makes it step\n\
                  down. Then it backfills the entry it missed.",
        },
        Step {
            say: "Check that all three nodes agree, byte for byte.",
            act: Act::Verify,
            why: "Same applied index, same Merkle root of the state machine. That hash is\n\
                  computed independently of the replication code, so it is a real check rather\n\
                  than the log agreeing with itself.",
        },
    ]
}

fn replication() -> Vec<Step> {
    vec![
        Step {
            say: "Wait for a leader, then watch a single write make its way through the log.",
            act: Act::WaitForLeader,
            why: "",
        },
        Step {
            say: "One SET. Watch the log lines: append, replicate, ack, commit, apply.",
            act: Act::Set {
                key: "name",
                value: "mohit",
                node: Target::Any,
            },
            why: "The order matters. The leader put it in its own log and fsynced it, sent it\n\
                  to both followers, waited for ONE of them to ack (that plus itself is a\n\
                  majority), then committed, applied, and only then answered the client.",
        },
        Step {
            say: "Ask a follower for the same key, without going through the leader.",
            act: Act::Get {
                key: "name",
                node: Target::Follower,
                stale: true,
            },
            why: "The follower has it, because commit is followed by apply on every node, not\n\
                  just the leader. This read is marked stale because nothing checked whether the\n\
                  follower is current — it just answered from its own state machine.",
        },
        Step {
            say: "Now look at what is actually stored in the log.",
            act: Act::InspectLog(Target::Leader),
            why: "Index 1 is a no-op the leader appends when it takes office; every leader does\n\
                  this so it has an entry from its own term to commit against. Your SET follows\n\
                  it. The log is the source of truth — the key-value map is just what you get by\n\
                  replaying it.",
        },
        Step {
            say: "And confirm every node ended up in the same place.",
            act: Act::Verify,
            why: "",
        },
    ]
}

fn partition() -> Vec<Step> {
    vec![
        Step {
            say: "This is the one worth watching closely: a leader that does not know it has\n\
                  been deposed. First, a normal write.",
            act: Act::Set {
                key: "a",
                value: "1",
                node: Target::Any,
            },
            why: "",
        },
        Step {
            say: "Now cut the leader off from both followers. Its process stays alive and it\n\
                  still believes it is the leader — it simply cannot be heard.",
            act: Act::PartitionLeader,
            why: "No connection was closed. Messages are being dropped in both directions,\n\
                  which is what a real network partition looks like from inside a process.",
        },
        Step {
            say: "Send a write to the isolated leader anyway.",
            act: Act::Set {
                key: "b",
                value: "2",
                node: Target::OldLeader,
            },
            why: "It appended the entry to its log — and then nothing. No majority can ack it,\n\
                  so it never commits, so the client never gets an OK. Note what it did NOT do:\n\
                  it did not answer success and it did not apply the value. An entry in a log is\n\
                  not a committed write.",
        },
        Step {
            say: "Meanwhile the other two have not heard a heartbeat in a while.",
            act: Act::WaitForNewLeader,
            why: "They elected a new leader in a higher term. There are now two nodes calling\n\
                  themselves leader — but only one of them can reach a majority, and that is the\n\
                  only one that can commit anything. This is why split brain does not corrupt\n\
                  the data.",
        },
        Step {
            say: "Write to the new leader, on the majority side.",
            act: Act::Set {
                key: "c",
                value: "3",
                node: Target::Leader,
            },
            why: "Committed normally — and it landed at the SAME log index that the isolated\n\
                  leader used for b=2. Two different commands, same slot, different terms.",
        },
        Step {
            say: "Look at the isolated leader's log, then the real leader's.",
            act: Act::InspectLog(Target::OldLeader),
            why: "The isolated node still shows b=2 sitting there UNCOMMITTED.",
        },
        Step {
            say: "And the majority side's log at that same index.",
            act: Act::InspectLog(Target::Leader),
            why: "Same index, different entry, higher term — and this one is committed.",
        },
        Step {
            say: "Now heal the partition and watch the conflict get resolved.",
            act: Act::Heal,
            why: "",
        },
        Step {
            say: "Give them a moment to reconcile.",
            act: Act::Sleep(Duration::from_millis(2500)),
            why: "The old leader saw a higher term and stepped down. The new leader's\n\
                  AppendEntries failed its consistency check, walked back, and told it to\n\
                  truncate. b=2 is gone — deleted from the log of the node that accepted it.",
        },
        Step {
            say: "So: does b exist?",
            act: Act::Get {
                key: "b",
                node: Target::Any,
                stale: false,
            },
            why: "No. That write was accepted by a leader, written to a log, and then\n\
                  destroyed. This is exactly why the client was never told it succeeded — and\n\
                  why 'the server wrote it down' and 'the write happened' are different claims.",
        },
        Step {
            say: "Everything else converged.",
            act: Act::Verify,
            why: "a=1 and c=3 survive on all three nodes. No committed value ever changed.",
        },
    ]
}

fn flaky() -> Vec<Step> {
    vec![
        Step {
            say: "A baseline first: how fast are writes on a healthy loopback cluster?",
            act: Act::Bench { ops: 60, conns: 4 },
            why: "",
        },
        Step {
            say: "Now make the network genuinely bad: drop 30% of packets and add 400ms of\n\
                  delay on every link.",
            act: Act::Fault {
                drop_rate: 0.3,
                delay_ms: 400,
            },
            why: "",
        },
        Step {
            say: "Write something and watch it get through anyway.",
            act: Act::Set {
                key: "through",
                value: "the-storm",
                node: Target::Any,
            },
            why: "Dropped AppendEntries are simply resent on the next heartbeat. Raft never\n\
                  needs a message to arrive; it only needs a majority to eventually agree, and\n\
                  retries are free because the same entry at the same index is idempotent.",
        },
        Step {
            say: "Run the same benchmark under the same bad network.",
            act: Act::Bench { ops: 60, conns: 4 },
            why: "Slower — often dramatically — but not wrong, and not stuck. This is the\n\
                  tradeoff Raft makes: it will give up liveness (progress) long before it gives\n\
                  up safety (correctness).",
        },
        Step {
            say: "Clear the faults.",
            act: Act::Heal,
            why: "",
        },
        Step {
            say: "And confirm nothing was corrupted by any of that.",
            act: Act::Verify,
            why: "Every node has the same state hash. Packet loss cost time, not consistency.",
        },
    ]
}

fn durability() -> Vec<Step> {
    vec![
        Step {
            say: "This cluster's fsync mode is whatever you started it with — check the log\n\
                  lines at startup. Write a value.",
            act: Act::Set {
                key: "survive",
                value: "me",
                node: Target::Any,
            },
            why: "",
        },
        Step {
            say: "Hard-kill a follower in the middle of things.",
            act: Act::Kill(Target::Follower),
            why: "TerminateProcess: no unwinding, no flush, no chance to write anything down.\n\
                  Whatever was in its log file is all it has.",
        },
        Step {
            say: "Keep writing while it is dead.",
            act: Act::WriteBurst(5),
            why: "Two of three is still a majority, so the cluster does not even slow down.",
        },
        Step {
            say: "Bring it back and watch it replay its own log from disk.",
            act: Act::Start(Target::Follower),
            why: "On startup it read its log file, checked the CRC on every record, and\n\
                  truncated any half-written tail. A torn record is safe to drop precisely\n\
                  because a write that was never finished was never acknowledged either.",
        },
        Step {
            say: "Then it catches up on what it missed.",
            act: Act::Verify,
            why: "Same hash as the others. Try this again with `dkv up --fsync never` and the\n\
                  recovery may lose the tail of the log — the node still converges, because the\n\
                  leader re-sends anything missing, but on a real power cut with every node set\n\
                  to `never` you can lose a committed write. That is the whole reason fsync\n\
                  costs what it costs.",
        },
    ]
}

fn snapshot() -> Vec<Step> {
    vec![
        Step {
            say: "Write enough entries to push the log past its compaction threshold.",
            act: Act::WriteBurst(70),
            why: "Watch for the SNAP lines: the state machine got written to a snapshot file,\n\
                  and only after that was durable did the log throw away the entries it covers.\n\
                  That order matters — the reverse would leave a node with neither.",
        },
        Step {
            say: "Kill a follower, then write more while it is gone.",
            act: Act::Kill(Target::Follower),
            why: "",
        },
        Step {
            say: "More writes, so the entries it needs get compacted away behind its back.",
            act: Act::WriteBurst(70),
            why: "The leader has now discarded the entries this follower would have needed.",
        },
        Step {
            say: "Bring it back. It cannot be caught up entry by entry any more.",
            act: Act::Start(Target::Follower),
            why: "The leader noticed that the next entry this follower needs no longer exists,\n\
                  so instead of replaying history it shipped the whole state machine as a\n\
                  snapshot. That is InstallSnapshot, and it is why a log can be compacted at all.",
        },
        Step {
            say: "Everyone agrees again.",
            act: Act::Verify,
            why: "",
        },
    ]
}

fn reads() -> Vec<Step> {
    vec![
        Step {
            say: "Write a value everyone can see.",
            act: Act::Set {
                key: "k",
                value: "before",
                node: Target::Any,
            },
            why: "",
        },
        Step {
            say: "Isolate the leader from both followers.",
            act: Act::PartitionLeader,
            why: "It still thinks it is leader. It has no idea anything changed.",
        },
        Step {
            say: "Let the majority elect someone else and move on.",
            act: Act::WaitForNewLeader,
            why: "",
        },
        Step {
            say: "Change the value on the new, real leader.",
            act: Act::Set {
                key: "k",
                value: "after",
                node: Target::Leader,
            },
            why: "Committed on the majority side. The isolated node knows nothing about it.",
        },
        Step {
            say: "Now do a STALE read from the isolated old leader.",
            act: Act::Get {
                key: "k",
                node: Target::OldLeader,
                stale: true,
            },
            why: "It answers instantly, confidently, and wrongly: 'before'. Nothing checked\n\
                  whether this node is still part of the cluster. This is what a stale read is,\n\
                  and it is a perfectly reasonable thing to want when you value latency over\n\
                  freshness.",
        },
        Step {
            say: "Same node, same key, but ask for a linearizable read.",
            act: Act::Get {
                key: "k",
                node: Target::OldLeader,
                stale: false,
            },
            why: "It refuses. Before answering, a leader must confirm with a majority that it\n\
                  is still the leader — and this one cannot reach anybody. Refusing to answer is\n\
                  the correct behaviour: the CAP tradeoff, made by one flag, visible in one\n\
                  command.",
        },
        Step {
            say: "Heal, and let the old leader rejoin.",
            act: Act::Heal,
            why: "",
        },
        Step {
            say: "Read the key once more.",
            act: Act::Get {
                key: "k",
                node: Target::Any,
                stale: false,
            },
            why: "'after' — the old leader caught up and the stale answer is gone.",
        },
    ]
}

fn dedup() -> Vec<Step> {
    vec![
        Step {
            say: "Retries are unavoidable: if a client sends a write and the connection drops\n\
                  before the reply, it cannot know whether the write happened. So it retries —\n\
                  and the cluster must not apply it twice.",
            act: Act::WaitForLeader,
            why: "",
        },
        Step {
            say: "Send a write with client id 4242, sequence 1.",
            act: Act::Set {
                key: "counter",
                value: "one",
                node: Target::Any,
            },
            why: "",
        },
        Step {
            say: "Now send the exact same (client, sequence) pair again, as a retry would.",
            act: Act::Nothing,
            why: "See the reply below — it commits a log entry, but the state machine\n\
                  recognises the sequence number as one it has already applied and returns the\n\
                  original answer without touching the map. The dedup table lives inside the\n\
                  state machine, not at the front door, because two leaders in different terms\n\
                  can both accept the same request — only apply order decides what really\n\
                  happened. It travels inside snapshots for the same reason.",
        },
    ]
}

// ---- the runner --------------------------------------------------------

pub struct DemoRunner {
    pub cluster: Cluster,
    pub narrate: mpsc::Sender<String>,
    pub auto: bool,
    pub speed: f64,
    last_leader: Option<NodeId>,
}

impl DemoRunner {
    pub fn new(cluster: Cluster, narrate: mpsc::Sender<String>, auto: bool, speed: f64) -> Self {
        DemoRunner {
            cluster,
            narrate,
            auto,
            speed: if speed <= 0.0 { 1.0 } else { speed },
            last_leader: None,
        }
    }

    pub async fn run(&mut self, name: &str, steps: Vec<Step>) {
        let total = steps.len();
        self.block(
            &format!("DEMO: {name}"),
            &format!(
                "{total} steps. {}",
                if self.auto {
                    "Running on a timer."
                } else {
                    "Press Enter to advance."
                }
            ),
        )
        .await;

        for (i, step) in steps.into_iter().enumerate() {
            self.block(&format!("[{}/{}]", i + 1, total), step.say)
                .await;
            self.pause_before_action().await;
            self.perform(&step.act).await;
            // Let the log lines the action caused actually arrive before the
            // explanation of them does.
            self.settle().await;
            if !step.why.is_empty() {
                self.block("why", step.why).await;
            }
            if i + 1 < total {
                self.wait_to_continue().await;
            }
        }
        self.block("done", "Run `dkv status`, `dkv inspect log` or `dkv watch` to poke at the\nresult, and `dkv down` when you are finished.").await;
    }

    async fn perform(&mut self, act: &Act) {
        match act {
            Act::Nothing => {
                // The dedup demo's point is made by sending the same request
                // twice with a fixed identity.
                let outcome = self
                    .cluster
                    .send_op(
                        None,
                        4242,
                        1,
                        Op::Set {
                            key: "counter".into(),
                            value: "two-but-ignored".into(),
                        },
                    )
                    .await;
                self.report("SET counter (retry of seq 1)", outcome).await;
            }
            Act::Set { key, value, node } => {
                let target = self.resolve(node).await;
                let (client, seq) = if *key == "counter" {
                    (4242, 1)
                } else {
                    (super::commands::new_client_id(), 1)
                };
                let outcome = self
                    .cluster
                    .send_op(
                        target,
                        client,
                        seq,
                        Op::Set {
                            key: key.to_string(),
                            value: value.to_string(),
                        },
                    )
                    .await;
                self.report(&format!("SET {key}={value}"), outcome).await;
            }
            Act::Get { key, node, stale } => {
                let target = self.resolve(node).await;
                let outcome = self
                    .cluster
                    .send_op(
                        target,
                        super::commands::new_client_id(),
                        1,
                        Op::Get {
                            key: key.to_string(),
                            stale: *stale,
                        },
                    )
                    .await;
                self.report(&format!("GET {key}"), outcome).await;
            }
            Act::WriteBurst(n) => {
                let client = super::commands::new_client_id();
                // Resolve the leader once. Letting every write rediscover it
                // costs a failed connection per write whenever a node is down,
                // which turns a quick burst into a minute of waiting.
                let leader = self.cluster.leader().await;
                let mut ok = 0;
                for i in 0..*n {
                    let r = self
                        .cluster
                        .send_op(
                            leader,
                            client,
                            (i + 1) as u64,
                            Op::Set {
                                key: format!("burst:{i}"),
                                value: format!("v{i}"),
                            },
                        )
                        .await;
                    if matches!(r, Ok(ref o) if matches!(o.reply, Reply::Committed { .. })) {
                        ok += 1;
                    }
                }
                self.say(&format!("wrote {ok}/{n} keys")).await;
            }
            Act::KillLeader => {
                let leader = self.cluster.leader().await;
                self.last_leader = leader;
                match leader {
                    Some(l) => self.kill(l).await,
                    None => self.say("no leader to kill right now").await,
                }
            }
            Act::Kill(target) => {
                if let Some(n) = self.resolve(target).await {
                    self.kill(n).await;
                }
            }
            Act::Start(target) => {
                let node = match target {
                    Target::OldLeader => self.last_leader,
                    other => self.resolve_down(other).await,
                };
                match node {
                    Some(n) => {
                        match super::supervise::control(
                            self.cluster.port_base,
                            crate::control::Control::Start { node: n },
                        )
                        .await
                        {
                            Ok(crate::control::Control::Ok { .. }) => {
                                self.say(&format!("started n{n}")).await
                            }
                            Ok(crate::control::Control::Error { message }) => {
                                self.say(&format!("could not start n{n}: {message}")).await
                            }
                            _ => {}
                        }
                    }
                    None => self.say("nothing to restart").await,
                }
            }
            Act::PartitionLeader => {
                let Some(leader) = self.cluster.leader().await else {
                    self.say("no leader to isolate").await;
                    return;
                };
                self.last_leader = Some(leader);
                for peer in self.cluster.nodes() {
                    if peer == leader {
                        continue;
                    }
                    self.cut(leader, peer, true).await;
                }
                self.say(&format!("n{leader} is now cut off from both peers"))
                    .await;
            }
            Act::Heal => {
                for a in self.cluster.nodes() {
                    for b in self.cluster.nodes() {
                        if a != b {
                            self.cut(a, b, false).await;
                        }
                    }
                    let _ = self
                        .cluster
                        .admin(
                            a,
                            AdminCmd::Fault {
                                peer: None,
                                drop_rate: None,
                                delay_ms: None,
                                clear: true,
                            },
                        )
                        .await;
                }
                self.say("all links healed").await;
            }
            Act::Fault {
                drop_rate,
                delay_ms,
            } => {
                for node in self.cluster.nodes() {
                    let _ = self
                        .cluster
                        .admin(
                            node,
                            AdminCmd::Fault {
                                peer: None,
                                drop_rate: Some(*drop_rate),
                                delay_ms: Some(*delay_ms),
                                clear: false,
                            },
                        )
                        .await;
                }
                self.say(&format!(
                    "every link now drops {:.0}% of messages and adds {delay_ms}ms",
                    drop_rate * 100.0
                ))
                .await;
            }
            Act::InspectLog(target) => {
                let node = match target {
                    Target::OldLeader => self.last_leader,
                    other => self.resolve(other).await,
                };
                if let Some(n) = node
                    && let Ok(reply) = self
                        .cluster
                        .admin(
                            n,
                            AdminCmd::InspectLog {
                                from: None,
                                to: None,
                            },
                        )
                        .await
                {
                    let mut text = String::new();
                    if let AdminReply::LogEntries {
                        entries,
                        first_index,
                        last_index,
                        commit_index,
                    } = &reply
                    {
                        text.push_str(&format!(
                            "n{n} log idx {first_index}..{last_index}, committed through {commit_index}\n"
                        ));
                        for e in entries {
                            text.push_str(&format!(
                                "  idx {:>3} term {:>2}  {:<11} {}\n",
                                e.index,
                                e.term,
                                if e.index <= *commit_index {
                                    "committed"
                                } else {
                                    "UNCOMMITTED"
                                },
                                e.command.summary()
                            ));
                        }
                    }
                    self.block("log", text.trim_end()).await;
                }
            }
            Act::Verify => {
                // Catching up is not instant — a node restarting from a
                // snapshot has a whole state machine to receive — so poll until
                // it converges rather than judging it on the first sample and
                // calling a healthy cluster broken.
                let deadline = std::time::Instant::now() + Duration::from_secs(20);
                let statuses = loop {
                    let statuses = self.cluster.status_all().await;
                    let up: Vec<_> = statuses.iter().filter_map(|(_, s)| s.as_ref()).collect();
                    let settled = up.len() == statuses.len()
                        && up.iter().all(|s| {
                            s.state_hash == up[0].state_hash && s.last_applied == up[0].last_applied
                        });
                    if settled || std::time::Instant::now() >= deadline {
                        break statuses;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                };
                let mut text = String::new();
                for (id, s) in &statuses {
                    match s {
                        Some(st) => text.push_str(&format!(
                            "n{id}  {:<9} term {:<3} applied {:<4} keys {:<4} hash {}\n",
                            st.role,
                            st.term,
                            st.last_applied,
                            st.keys,
                            super::commands::fmt_hash(st.state_hash)
                        )),
                        None => text.push_str(&format!("n{id}  DOWN\n")),
                    }
                }
                let up: Vec<_> = statuses.iter().filter_map(|(_, s)| s.as_ref()).collect();
                let converged = !up.is_empty()
                    && up.iter().all(|s| {
                        s.state_hash == up[0].state_hash && s.last_applied == up[0].last_applied
                    });
                text.push_str(if converged {
                    "=> CONVERGED: identical applied index and state hash"
                } else {
                    "=> not converged yet"
                });
                self.block("verify", text.trim_end()).await;
            }
            Act::WaitForLeader => {
                match self.cluster.wait_for_leader(Duration::from_secs(10)).await {
                    Some(l) => {
                        self.last_leader = Some(l);
                        self.say(&format!("n{l} is leader")).await;
                    }
                    None => self.say("no leader emerged within 10s").await,
                }
            }
            Act::WaitForNewLeader => {
                let old = self.last_leader;
                let deadline = std::time::Instant::now() + Duration::from_secs(15);
                loop {
                    if let Some(l) = self.cluster.leader().await
                        && Some(l) != old
                    {
                        self.say(&format!("n{l} is the new leader")).await;
                        self.last_leader = Some(l);
                        break;
                    }
                    if std::time::Instant::now() >= deadline {
                        self.say("no new leader within 15s").await;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            Act::Sleep(d) => {
                tokio::time::sleep(Duration::from_secs_f64(d.as_secs_f64() / self.speed)).await
            }
            Act::Bench { ops, conns } => {
                let result = super::bench::run_once(&self.cluster, *ops, *conns, 32).await;
                self.block(
                    "bench",
                    &format!(
                        "{} writes, {} errors, {:.0} ops/s\n  p50 {:.1}ms   p95 {:.1}ms   p99 {:.1}ms",
                        result.ops,
                        result.errors,
                        result.throughput(),
                        result.percentile(50.0),
                        result.percentile(95.0),
                        result.percentile(99.0)
                    ),
                )
                .await;
            }
        }
    }

    async fn kill(&mut self, node: NodeId) {
        match super::supervise::control(
            self.cluster.port_base,
            crate::control::Control::Kill { node },
        )
        .await
        {
            Ok(crate::control::Control::Ok { .. }) => {
                self.say(&format!("killed n{node} (hard)")).await;
            }
            Ok(crate::control::Control::Error { message }) => {
                self.say(&format!("could not kill n{node}: {message}"))
                    .await
            }
            _ => {}
        }
    }

    async fn cut(&self, a: NodeId, b: NodeId, on: bool) {
        // Both endpoints, so the split is symmetric even though either side
        // alone would be enough to stop the traffic.
        let _ = self
            .cluster
            .admin(a, AdminCmd::Partition { peer: b, on })
            .await;
        let _ = self
            .cluster
            .admin(b, AdminCmd::Partition { peer: a, on })
            .await;
    }

    async fn resolve(&mut self, target: &Target) -> Option<NodeId> {
        match target {
            Target::Any => None,
            Target::Node(n) => Some(*n),
            Target::Leader => {
                let l = self.cluster.leader().await;
                if l.is_some() {
                    self.last_leader = l;
                }
                l
            }
            Target::Follower => {
                let statuses = self.cluster.status_all().await;
                statuses
                    .iter()
                    .filter_map(|(_, s)| s.as_ref())
                    .find(|s| s.role != "LEADER")
                    .map(|s| s.id)
            }
            Target::OldLeader => self.last_leader,
        }
    }

    /// For restarts: the node that is currently *not* answering.
    async fn resolve_down(&mut self, target: &Target) -> Option<NodeId> {
        if let Target::Node(n) = target {
            return Some(*n);
        }
        let statuses = self.cluster.status_all().await;
        statuses
            .iter()
            .find(|(_, s)| s.is_none())
            .map(|(id, _)| *id)
    }

    async fn report(
        &self,
        what: &str,
        outcome: Result<super::commands::SendOutcome, crate::types::ClientError>,
    ) {
        let text = match outcome {
            Ok(o) => match o.reply {
                Reply::Committed {
                    index,
                    term,
                    ms,
                    duplicate,
                } => format!(
                    "{what} -> n{} OK, committed at idx {index} term {term} in {ms}ms{}",
                    o.node,
                    if duplicate {
                        "  [DUPLICATE — returned the original answer, map untouched]"
                    } else {
                        ""
                    }
                ),
                Reply::Value {
                    value,
                    index,
                    stale,
                } => format!(
                    "{what} -> n{} = {}  ({} @ idx {index})",
                    o.node,
                    value.unwrap_or_else(|| "<not found>".into()),
                    if stale { "stale read" } else { "linearizable" }
                ),
                Reply::Error { kind, message, .. } => {
                    format!("{what} -> n{} REFUSED: {kind:?} — {message}", o.node)
                }
            },
            Err(e) => format!("{what} -> failed: {e}"),
        };
        self.say(&text).await;
    }

    async fn say(&self, text: &str) {
        let _ = self
            .narrate
            .send(self.cluster.paint("1;36", &format!("      >> {text}")))
            .await;
    }

    async fn block(&self, title: &str, body: &str) {
        let mut out = String::new();
        out.push('\n');
        out.push_str(&self.cluster.paint("1;36", &format!("  {}", "=".repeat(72))));
        out.push('\n');
        out.push_str(
            &self
                .cluster
                .paint("1;36", &format!("  {}", title.to_uppercase())),
        );
        out.push('\n');
        for line in body.lines() {
            out.push_str(&self.cluster.paint("36", &format!("  {line}")));
            out.push('\n');
        }
        out.push_str(&self.cluster.paint("1;36", &format!("  {}", "=".repeat(72))));
        let _ = self.narrate.send(out).await;
    }

    async fn pause_before_action(&self) {
        tokio::time::sleep(Duration::from_secs_f64(0.6 / self.speed)).await;
    }

    /// Node logs are produced asynchronously; without this the "why" block can
    /// print before the lines it is explaining.
    async fn settle(&self) {
        tokio::time::sleep(Duration::from_secs_f64(1.2 / self.speed)).await;
    }

    async fn wait_to_continue(&self) {
        if self.auto {
            tokio::time::sleep(Duration::from_secs_f64(2.5 / self.speed)).await;
            return;
        }
        let _ = self
            .narrate
            .send(self.cluster.dim("      (press Enter to continue)"))
            .await;
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        let _ = lines.next_line().await;
    }
}

/// Starts a cluster for a demo that has none, or attaches to the running one.
pub async fn ensure_cluster(opts: NodeOptions, fresh: bool) -> Result<Option<Supervisor>, String> {
    if super::supervise::supervisor_is_running(opts.port_base).await {
        return Ok(None);
    }
    Supervisor::start(opts, fresh).await.map(Some)
}
