//! The wire protocol, in one file, on purpose: this is the document you read to
//! understand what the cluster actually says to itself.
//!
//! Every message is one line of JSON terminated by `\n`. That is slower and
//! fatter than a binary format, and it buys something worth more here — you can
//! point netcat at a node and read the conversation:
//!
//! ```text
//! $ echo '{"type":"Request","client_id":9,"seq":1,"op":{"op":"Get","key":"name","stale":false}}' | nc 127.0.0.1 6101
//! ```

use crate::types::{LogEntry, LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    // ---- Raft RPCs (Figure 2) ----
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteReply {
        term: Term,
        from: NodeId,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        /// Echoed back untouched. A linearizable read may only be answered once
        /// a majority has replied to a round started *after* the read arrived;
        /// without this tag, a reply already in flight when the read landed
        /// would be miscounted as proof of current leadership — which is the
        /// exact stale read ReadIndex exists to prevent.
        read_round: u64,
    },
    AppendEntriesReply {
        term: Term,
        from: NodeId,
        success: bool,
        read_round: u64,
        /// Highest index the follower now agrees with. Only meaningful on success.
        match_index: LogIndex,
        /// On rejection, where the follower's log first disagrees. Without this
        /// the leader walks back one index per round trip, which takes a full
        /// heartbeat interval per entry on a long divergence.
        conflict_index: LogIndex,
        conflict_term: Term,
    },
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        snapshot: SnapshotPayload,
    },
    InstallSnapshotReply {
        term: Term,
        from: NodeId,
        last_included_index: LogIndex,
    },

    // ---- Client API ----
    Request {
        /// Identifies the client across retries. Paired with `seq` it is what
        /// makes a retried write idempotent.
        client_id: u64,
        seq: u64,
        op: Op,
    },
    Reply(Reply),

    // ---- Admin / inspection ----
    Admin(AdminCmd),
    AdminReply(AdminReply),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Op {
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Get {
        key: String,
        /// `true` reads this node's state machine directly with no leadership
        /// check: fast, local, and possibly out of date. The knob that makes a
        /// stale read something you can see rather than read about.
        stale: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "reply")]
pub enum Reply {
    /// A write committed and applied.
    Committed {
        index: LogIndex,
        term: Term,
        ms: u64,
        /// `true` when the dedup table recognised this `(client_id, seq)` and
        /// returned the original result instead of applying it again.
        duplicate: bool,
    },
    Value {
        value: Option<String>,
        /// The commit index the read was served at; with `stale` it is whatever
        /// this node had applied.
        index: LogIndex,
        stale: bool,
    },
    Error {
        kind: ErrorKind,
        message: String,
        leader_hint: Option<NodeId>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    NotLeader,
    LostLeadership,
    ReadTimeout,
    Internal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "admin")]
pub enum AdminCmd {
    Status,
    InspectLog {
        from: Option<LogIndex>,
        to: Option<LogIndex>,
    },
    InspectState,
    InspectSnapshot,
    /// A hard partition of the link to `peer`: messages in both directions are
    /// dropped, while the process stays alive and keeps believing whatever it
    /// believed. That gap is the whole lesson.
    Partition {
        peer: NodeId,
        on: bool,
    },
    Fault {
        peer: Option<NodeId>,
        drop_rate: Option<f64>,
        delay_ms: Option<u64>,
        clear: bool,
    },
    Compact,
    StepDown,
    /// Force an election timeout now. Demos use it so they do not have to wait.
    Campaign,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "adminReply")]
pub enum AdminReply {
    Status(Box<NodeStatus>),
    LogEntries {
        entries: Vec<LogEntry>,
        first_index: LogIndex,
        last_index: LogIndex,
        commit_index: LogIndex,
    },
    State {
        kv: BTreeMap<String, String>,
        last_applied: LogIndex,
        state_hash: u32,
        clients: usize,
    },
    Snapshot {
        last_included_index: LogIndex,
        last_included_term: Term,
        keys: usize,
        bytes: u64,
    },
    Ok,
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub role: String,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub first_index: LogIndex,
    pub last_index: LogIndex,
    pub log_entries: u64,
    pub keys: usize,
    /// Merkle root of the applied state machine. Two nodes at the same
    /// `last_applied` with different roots have diverged, which is the bug
    /// Raft exists to prevent.
    pub state_hash: u32,
    pub snapshot_index: LogIndex,
    pub fsync: String,
    pub peers: Vec<PeerStatus>,
    pub faults: Vec<FaultStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerStatus {
    pub id: NodeId,
    pub reachable: bool,
    /// Milliseconds since this peer last answered anything.
    pub last_ack_ms: Option<u64>,
    pub next_index: LogIndex,
    pub match_index: LogIndex,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FaultStatus {
    pub peer: NodeId,
    pub partitioned: bool,
    pub drop_rate: f64,
    pub delay_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotPayload {
    pub kv: BTreeMap<String, String>,
    /// Dedup table travels with the snapshot. Leaving it out means a restored
    /// node re-applies a retried write and silently doubles it.
    ///
    /// A list rather than a map keyed by client id, and not for style: JSON
    /// object keys are strings, so `serde_json` happily *writes* a
    /// `BTreeMap<u64, _>` and then refuses to *read* it back ("invalid type:
    /// string, expected u64"). Every snapshot would be undecodable on arrival,
    /// and since an undecodable frame is just dropped, the only symptom is a
    /// follower that never catches up while the leader resends forever.
    pub clients: Vec<ClientRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientRecord {
    pub client_id: u64,
    pub seq: u64,
    pub value: Option<String>,
}

/// The same thing as a `ClientRecord`, in the shape the state machine wants for
/// lookups. Kept separate so the wire format is free to differ from the map.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DedupEntry {
    pub seq: u64,
    pub value: Option<String>,
}

impl Message {
    /// The term carried by any Raft RPC, request or reply. The step-down rule
    /// applies to *every* message that carries a term, which is exactly why this
    /// is one function and not a check written out at each call site.
    pub fn term(&self) -> Option<Term> {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotReply { term, .. } => Some(*term),
            _ => None,
        }
    }

    /// Which peer sent this, when that is knowable from the message itself.
    pub fn sender(&self) -> Option<NodeId> {
        match self {
            Message::RequestVote { candidate_id, .. } => Some(*candidate_id),
            Message::AppendEntries { leader_id, .. } => Some(*leader_id),
            Message::InstallSnapshot { leader_id, .. } => Some(*leader_id),
            Message::RequestVoteReply { from, .. }
            | Message::AppendEntriesReply { from, .. }
            | Message::InstallSnapshotReply { from, .. } => Some(*from),
            _ => None,
        }
    }

    pub fn encode(&self) -> String {
        // A message that cannot be serialised is a programming error, not a
        // runtime condition — there is no remote to report it to.
        serde_json::to_string(self).expect("every Message is serialisable")
    }

    pub fn decode(line: &str) -> Result<Message, String> {
        serde_json::from_str(line).map_err(|e| format!("{e}"))
    }

    /// Short label for trace logs — the full JSON is too wide to scan.
    pub fn label(&self) -> &'static str {
        match self {
            Message::RequestVote { .. } => "RequestVote",
            Message::RequestVoteReply { .. } => "RequestVoteReply",
            Message::AppendEntries { .. } => "AppendEntries",
            Message::AppendEntriesReply { .. } => "AppendEntriesReply",
            Message::InstallSnapshot { .. } => "InstallSnapshot",
            Message::InstallSnapshotReply { .. } => "InstallSnapshotReply",
            Message::Request { .. } => "Request",
            Message::Reply(_) => "Reply",
            Message::Admin(_) => "Admin",
            Message::AdminReply(_) => "AdminReply",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;

    #[test]
    fn a_message_survives_a_round_trip_through_json() {
        let msg = Message::AppendEntries {
            term: 4,
            leader_id: 2,
            prev_log_index: 6,
            prev_log_term: 3,
            entries: vec![LogEntry {
                index: 7,
                term: 4,
                client_id: 11,
                seq: 1,
                command: Command::Set {
                    key: "name".into(),
                    value: "mohit".into(),
                },
            }],
            leader_commit: 6,
            read_round: 0,
        };
        let back = Message::decode(&msg.encode()).expect("decodes");
        match back {
            Message::AppendEntries { term, entries, .. } => {
                assert_eq!(term, 4);
                assert_eq!(entries[0].command.summary(), "SET name=mohit");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn every_raft_rpc_exposes_its_term_so_the_step_down_rule_cannot_be_forgotten() {
        let rpcs = vec![
            Message::RequestVote {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            },
            Message::RequestVoteReply {
                term: 2,
                from: 2,
                vote_granted: true,
            },
            Message::AppendEntriesReply {
                term: 3,
                from: 3,
                success: false,
                read_round: 0,
                match_index: 0,
                conflict_index: 1,
                conflict_term: 0,
            },
            Message::InstallSnapshotReply {
                term: 4,
                from: 2,
                last_included_index: 9,
            },
        ];
        for rpc in rpcs {
            assert!(rpc.term().is_some(), "{} hides its term", rpc.label());
        }
    }

    #[test]
    fn a_client_request_carries_no_term_because_clients_have_no_opinion_about_terms() {
        let req = Message::Request {
            client_id: 1,
            seq: 1,
            op: Op::Get {
                key: "k".into(),
                stale: false,
            },
        };
        assert!(req.term().is_none());
    }

    #[test]
    fn an_install_snapshot_survives_the_wire_dedup_table_and_all() {
        // This is a regression test for a bug that produced no error anywhere:
        // the dedup table used to be a map keyed by client id, serde_json wrote
        // the keys as JSON strings (they have to be), and then refused to read
        // them back as integers. The frame was dropped on arrival as
        // undecodable, so the leader resent the snapshot every heartbeat and the
        // follower sat at the same applied index forever.
        let mut kv = BTreeMap::new();
        kv.insert("name".to_string(), "mohit".to_string());
        let msg = Message::InstallSnapshot {
            term: 4,
            leader_id: 2,
            last_included_index: 130,
            last_included_term: 3,
            snapshot: SnapshotPayload {
                kv,
                clients: vec![ClientRecord {
                    client_id: 18_446_744_073_709_551_000,
                    seq: 7,
                    value: Some("cached".into()),
                }],
            },
        };
        match Message::decode(&msg.encode()).expect("an InstallSnapshot must decode") {
            Message::InstallSnapshot { snapshot, .. } => {
                assert_eq!(snapshot.kv["name"], "mohit");
                assert_eq!(snapshot.clients[0].client_id, 18_446_744_073_709_551_000);
                assert_eq!(snapshot.clients[0].seq, 7);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn encoded_messages_are_one_line_so_newline_framing_is_safe() {
        let msg = Message::Request {
            client_id: 1,
            seq: 1,
            op: Op::Set {
                key: "multi\nline".into(),
                value: "also\nnewline".into(),
            },
        };
        let encoded = msg.encode();
        assert!(
            !encoded.contains('\n'),
            "serde_json escapes newlines inside strings, so a framed line stays one line"
        );
    }
}
