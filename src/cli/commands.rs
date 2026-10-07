//! The client side: one connection per command, follow the leader hint, print
//! something a human can read.
//!
//! The CLI keeps no state between invocations. Every `dkv get` is a fresh
//! process that connects, asks, prints, exits. That is slower than a persistent
//! client and it means the redirect dance happens every time — which is the
//! point: watching `not leader, retrying n2` teaches more than hiding it would.

use crate::config::DEFAULT_PORT_BASE;
use crate::logfmt::{RESET, node_color};
use crate::proto::{AdminCmd, AdminReply, ErrorKind, Message, NodeStatus, Op, Reply};
use crate::transport;
use crate::types::{ClientError, LogEntry, NodeId};
use rand::Rng;
use std::time::{Duration, Instant};

/// Generous, because a write on the minority side of a partition legitimately
/// takes the node's full waiter timeout before it gives up.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const ADMIN_TIMEOUT: Duration = Duration::from_millis(1500);
const MAX_REDIRECTS: usize = 6;

#[derive(Clone, Copy, Debug)]
pub struct Cluster {
    pub port_base: u16,
    pub color: bool,
}

impl Default for Cluster {
    fn default() -> Self {
        Cluster {
            port_base: DEFAULT_PORT_BASE,
            color: true,
        }
    }
}

pub struct SendOutcome {
    pub node: NodeId,
    pub reply: Reply,
    /// Nodes tried before this one, and why each was passed over. "Not leader"
    /// and "did not answer" look the same to a user and mean quite different
    /// things about the cluster, so they are not collapsed together.
    pub redirects: Vec<(NodeId, Detour)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Detour {
    NotLeader,
    Unreachable,
}

impl Detour {
    pub fn describe(&self) -> &'static str {
        match self {
            Detour::NotLeader => "not leader, retrying",
            Detour::Unreachable => "no answer, trying the next node",
        }
    }
}

impl Cluster {
    pub fn addr(&self, node: NodeId) -> String {
        format!("127.0.0.1:{}", self.port_base + node as u16)
    }

    pub fn control_addr(&self) -> String {
        format!("127.0.0.1:{}", self.port_base)
    }

    pub fn nodes(&self) -> [NodeId; 3] {
        crate::config::CLUSTER
    }

    // ---- painting ----

    pub fn tag(&self, node: NodeId) -> String {
        if self.color {
            format!("{}[n{node}]{RESET}", node_color(node))
        } else {
            format!("[n{node}]")
        }
    }

    pub fn paint(&self, code: &str, s: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{s}{RESET}")
        } else {
            s.to_string()
        }
    }

    pub fn good(&self, s: &str) -> String {
        self.paint("32", s)
    }

    pub fn bad(&self, s: &str) -> String {
        self.paint("31", s)
    }

    pub fn warn(&self, s: &str) -> String {
        self.paint("33", s)
    }

    pub fn dim(&self, s: &str) -> String {
        self.paint("2;37", s)
    }

    pub fn bold(&self, s: &str) -> String {
        self.paint("1", s)
    }

    // ---- client operations ----

    /// Sends one operation. With `prefer` set the request goes to that node and
    /// nowhere else — the user asked for it, possibly to talk to a node that is
    /// partitioned and still calling itself leader. Without it, the CLI follows
    /// `NotLeader` hints and walks the ring on connection failures.
    pub async fn send_op(
        &self,
        prefer: Option<NodeId>,
        client_id: u64,
        seq: u64,
        op: Op,
    ) -> Result<SendOutcome, ClientError> {
        let mut target = prefer.unwrap_or(1);
        let mut redirects = Vec::new();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let msg = Message::Request {
                client_id,
                seq,
                op: op.clone(),
            };
            match transport::request(&self.addr(target), msg, REQUEST_TIMEOUT).await {
                Ok(Message::Reply(Reply::Error {
                    kind: ErrorKind::NotLeader,
                    leader_hint,
                    ..
                })) if prefer.is_none() && attempts < MAX_REDIRECTS => {
                    redirects.push((target, Detour::NotLeader));
                    match leader_hint {
                        Some(hint) if hint != target => target = hint,
                        _ => {
                            // No leader known: an election is probably in
                            // progress. Give it a moment, try the next node.
                            tokio::time::sleep(Duration::from_millis(400)).await;
                            target = next_node(target);
                        }
                    }
                }
                Ok(Message::Reply(reply)) => {
                    return Ok(SendOutcome {
                        node: target,
                        reply,
                        redirects,
                    });
                }
                Ok(other) => {
                    return Err(ClientError::Transport(format!(
                        "unexpected {} from n{target}",
                        other.label()
                    )));
                }
                Err(ClientError::Transport(_)) | Err(ClientError::Timeout)
                    if prefer.is_none() && attempts < MAX_REDIRECTS =>
                {
                    // That node is down or unreachable. Walk on quickly — the
                    // connect attempt already cost its own timeout.
                    redirects.push((target, Detour::Unreachable));
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    target = next_node(target);
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn admin(&self, node: NodeId, cmd: AdminCmd) -> Result<AdminReply, ClientError> {
        match transport::request(&self.addr(node), Message::Admin(cmd), ADMIN_TIMEOUT).await? {
            Message::AdminReply(reply) => Ok(reply),
            other => Err(ClientError::Transport(format!(
                "unexpected {} from n{node}",
                other.label()
            ))),
        }
    }

    pub async fn status(&self, node: NodeId) -> Option<NodeStatus> {
        match self.admin(node, AdminCmd::Status).await {
            Ok(AdminReply::Status(s)) => Some(*s),
            _ => None,
        }
    }

    /// All three at once. A node that does not answer within the admin timeout
    /// is reported as `None`, which the callers render as DOWN.
    pub async fn status_all(&self) -> Vec<(NodeId, Option<NodeStatus>)> {
        let (a, b, c) = tokio::join!(self.status(1), self.status(2), self.status(3));
        vec![(1, a), (2, b), (3, c)]
    }

    /// The node most likely to really be leader right now.
    ///
    /// During a partition more than one node calls itself leader, and the stale
    /// one answers just as confidently as the real one. The tie-break is the
    /// term: a higher term always wins in Raft, so the highest-term claimant is
    /// the one worth talking to. Taking the first claimant instead would mean
    /// forever picking the isolated node that has not noticed it was deposed.
    pub async fn leader(&self) -> Option<NodeId> {
        self.status_all()
            .await
            .into_iter()
            .filter_map(|(_, s)| s)
            .filter(|s| s.role == "LEADER")
            .max_by_key(|s| s.term)
            .map(|s| s.id)
    }

    /// Waits until some node calls itself leader and at least one other node
    /// agrees. Demos use this rather than sleeping a guessed number of seconds.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Option<NodeId> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let statuses = self.status_all().await;
            let leaders: Vec<NodeId> = statuses
                .iter()
                .filter_map(|(_, s)| s.as_ref())
                .filter(|s| s.role == "LEADER")
                .map(|s| s.id)
                .collect();
            if let Some(leader) = leaders.first() {
                let agreed = statuses
                    .iter()
                    .filter_map(|(_, s)| s.as_ref())
                    .filter(|s| s.leader == Some(*leader))
                    .count();
                if agreed >= 2 {
                    return Some(*leader);
                }
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        None
    }

    // ---- printing ----

    pub fn print_status_table(&self, statuses: &[(NodeId, Option<NodeStatus>)]) {
        println!(
            "{}",
            self.dim(
                "node  role       term  commit  applied  log        keys  hash      leader  peers"
            )
        );
        for (id, status) in statuses {
            match status {
                Some(s) => {
                    let role = match s.role.as_str() {
                        "LEADER" => self.good(&format!("{:<9}", s.role)),
                        "CANDIDATE" => self.warn(&format!("{:<9}", s.role)),
                        _ => format!("{:<9}", s.role),
                    };
                    let peers: Vec<String> = s
                        .peers
                        .iter()
                        .map(|p| {
                            if p.reachable {
                                self.good(&format!("n{}", p.id))
                            } else {
                                self.bad(&format!("n{}", p.id))
                            }
                        })
                        .collect();
                    let faults = if s.faults.is_empty() {
                        String::new()
                    } else {
                        let list: Vec<String> = s
                            .faults
                            .iter()
                            .map(|f| {
                                if f.partitioned {
                                    format!("cut->n{}", f.peer)
                                } else {
                                    format!("n{} drop={} +{}ms", f.peer, f.drop_rate, f.delay_ms)
                                }
                            })
                            .collect();
                        self.warn(&format!("  faults: {}", list.join(", ")))
                    };
                    println!(
                        "{}  {}  {:>4}  {:>6}  {:>7}  {:>4}..{:<4} {:>4}  {}  {:<6}  {}{}",
                        self.tag(*id),
                        role,
                        s.term,
                        s.commit_index,
                        s.last_applied,
                        s.first_index,
                        s.last_index,
                        s.keys,
                        fmt_hash(s.state_hash),
                        s.leader
                            .map(|l| format!("n{l}"))
                            .unwrap_or_else(|| "-".into()),
                        peers.join(" "),
                        faults
                    );
                }
                None => println!("{}  {}", self.tag(*id), self.bad("DOWN (no answer)")),
            }
        }
    }

    pub fn print_log(&self, node: NodeId, reply: &AdminReply) {
        if let AdminReply::LogEntries {
            entries,
            first_index,
            last_index,
            commit_index,
        } = reply
        {
            println!(
                "{} log idx {}..{}  commit {}  ({} entries shown)",
                self.tag(node),
                first_index,
                last_index,
                commit_index,
                entries.len()
            );
            for e in entries {
                self.print_entry(e, *commit_index);
            }
        }
    }

    fn print_entry(&self, e: &LogEntry, commit_index: u64) {
        let state = if e.index <= commit_index {
            self.good("committed  ")
        } else {
            self.warn("UNCOMMITTED")
        };
        let who = if e.is_internal() {
            self.dim("internal")
        } else {
            self.dim(&format!("client {} seq {}", e.client_id, e.seq))
        };
        println!(
            "  idx {:>4}  term {:>3}  {}  {:<28} {}",
            e.index,
            e.term,
            state,
            e.command.summary(),
            who
        );
    }

    /// The convergence check. Converged means every reachable node reports the
    /// same `last_applied` and the same Merkle root. Prints the table, returns
    /// the verdict.
    pub async fn verify(&self, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        loop {
            let statuses = self.status_all().await;
            let up: Vec<&NodeStatus> = statuses.iter().filter_map(|(_, s)| s.as_ref()).collect();
            let converged = !up.is_empty()
                && up.iter().all(|s| {
                    s.last_applied == up[0].last_applied && s.state_hash == up[0].state_hash
                });
            let all_up = up.len() == statuses.len();
            let done = (converged && all_up) || Instant::now() >= deadline;
            if done {
                self.print_status_table(&statuses);
                if converged && all_up {
                    println!(
                        "{}",
                        self.good(&format!(
                            "CONVERGED — all {} nodes at applied idx {} with state hash {}",
                            up.len(),
                            up[0].last_applied,
                            fmt_hash(up[0].state_hash)
                        ))
                    );
                } else if converged {
                    println!(
                        "{}",
                        self.warn(&format!(
                            "{} of {} nodes agree (hash {}); the rest are down",
                            up.len(),
                            statuses.len(),
                            fmt_hash(up[0].state_hash)
                        ))
                    );
                } else {
                    println!(
                        "{}",
                        self.bad("DIVERGED — nodes disagree on applied index or state hash")
                    );
                }
                return converged && all_up;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// Prints a client reply the way a person would want to read it.
    pub fn print_outcome(&self, op_desc: &str, outcome: &SendOutcome) {
        for (node, detour) in &outcome.redirects {
            println!("{} {}", self.tag(*node), self.dim(detour.describe()));
        }
        match &outcome.reply {
            Reply::Committed {
                index,
                term,
                ms,
                duplicate,
            } => {
                let dup = if *duplicate {
                    self.warn("  (duplicate — already applied, not reapplied)")
                } else {
                    String::new()
                };
                println!(
                    "{} {} {}{}",
                    self.tag(outcome.node),
                    self.good("OK"),
                    self.dim(&format!(
                        "{op_desc}  committed at idx {index} term {term} in {ms}ms"
                    )),
                    dup
                );
            }
            Reply::Value {
                value,
                index,
                stale,
            } => {
                let shown = match value {
                    Some(v) => self.bold(v),
                    None => self.dim("<not found>"),
                };
                let how = if *stale {
                    self.warn(&format!("stale read @ applied idx {index}"))
                } else {
                    self.dim(&format!("linearizable @ commit idx {index}"))
                };
                println!("{} {} {}", self.tag(outcome.node), shown, how);
            }
            Reply::Error {
                kind,
                message,
                leader_hint,
            } => {
                let hint = leader_hint
                    .map(|h| format!(" (leader hint: n{h})"))
                    .unwrap_or_default();
                println!(
                    "{} {} {}{}",
                    self.tag(outcome.node),
                    self.bad(&format!("{kind:?}")),
                    message,
                    self.dim(&hint)
                );
            }
        }
    }
}

pub fn next_node(node: NodeId) -> NodeId {
    if node >= 3 { 1 } else { node + 1 }
}

pub fn fmt_hash(hash: u32) -> String {
    format!("{hash:08x}")
}

/// A fresh identity for this CLI invocation. Retries of the *same* logical
/// request must reuse the id (the dedup demo passes it explicitly).
pub fn new_client_id() -> u64 {
    rand::rng().random_range(1..u64::MAX)
}

pub fn describe_reply_kind(reply: &Reply) -> &'static str {
    match reply {
        Reply::Committed { .. } => "committed",
        Reply::Value { .. } => "value",
        Reply::Error { .. } => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walking_the_ring_wraps_from_the_last_node_back_to_the_first() {
        assert_eq!(next_node(1), 2);
        assert_eq!(next_node(2), 3);
        assert_eq!(next_node(3), 1);
    }

    #[test]
    fn state_hashes_print_as_fixed_width_hex_so_they_line_up_in_the_table() {
        assert_eq!(fmt_hash(0), "00000000");
        assert_eq!(fmt_hash(0xdead_beef), "deadbeef");
    }

    #[test]
    fn a_client_id_is_never_the_reserved_internal_zero() {
        for _ in 0..1000 {
            assert_ne!(new_client_id(), 0);
        }
    }
}
