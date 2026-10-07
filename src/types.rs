//! The vocabulary every other module speaks: ids, terms, log entries, errors.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Nodes are 1, 2, 3. Small and human-typeable on purpose — every log line and
/// every CLI flag carries one, and `n3` reads better than a uuid.
pub type NodeId = u8;

/// Raft's logical clock. Only ever moves forward, and a higher one always wins.
pub type Term = u64;

/// 1-based. Index 0 is the sentinel "no entry", which is why the log never
/// stores an entry there.
pub type LogIndex = u64;

/// What a client asked the cluster to do. This is the payload that gets
/// replicated; everything else in a log entry is bookkeeping.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Command {
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Appended by a leader as its first act each term. It carries no user data
    /// but it is what lets the leader commit entries inherited from older terms
    /// without violating the Figure 8 rule, and it gives ReadIndex something
    /// from the current term to anchor on.
    Noop,
}

impl Command {
    pub fn summary(&self) -> String {
        match self {
            Command::Set { key, value } => format!("SET {key}={value}"),
            Command::Delete { key } => format!("DEL {key}"),
            Command::Noop => "NOOP".to_string(),
        }
    }
}

/// One replicated log record. `client_id` 0 means "internal, not deduplicated" —
/// only real client writes carry an id and a sequence number.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub client_id: u64,
    pub seq: u64,
    pub command: Command,
}

impl LogEntry {
    pub fn is_internal(&self) -> bool {
        self.client_id == 0
    }
}

/// What applying an entry to the state machine produced. Cached per client so a
/// retried request returns the original answer instead of being applied twice.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result")]
pub enum ApplyResult {
    Ok,
    /// `None` is a genuine "key not present", not an error.
    Value {
        value: Option<String>,
    },
}

#[derive(Debug)]
pub enum StoreError {
    Io(std::io::Error),
    /// The record header promised more bytes than the file holds. Expected after
    /// a hard kill mid-append; the tail gets truncated.
    TornRecord {
        offset: u64,
    },
    /// Header and payload disagree. Same treatment as a torn record.
    BadChecksum {
        offset: u64,
        index: LogIndex,
    },
    /// Entry indexes are not contiguous. Means the file was edited or a previous
    /// truncation went wrong.
    IndexGap {
        expected: LogIndex,
        found: LogIndex,
    },
    /// The log's first entry does not continue from the snapshot, so there is a
    /// hole in this node's history. Unlike a torn tail this is not recoverable:
    /// coming up would mean serving a state machine that is quietly missing
    /// entries, so the node refuses to start instead.
    LogDoesNotFollowSnapshot {
        first_index: LogIndex,
        snapshot_at: LogIndex,
    },
    Decode(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "io error: {e}"),
            StoreError::TornRecord { offset } => {
                write!(f, "torn record at byte {offset} (partial write)")
            }
            StoreError::BadChecksum { offset, index } => {
                write!(f, "checksum mismatch at byte {offset} (entry {index})")
            }
            StoreError::IndexGap { expected, found } => {
                write!(f, "log index gap: expected {expected}, found {found}")
            }
            StoreError::LogDoesNotFollowSnapshot {
                first_index,
                snapshot_at,
            } => write!(
                f,
                "log starts at index {first_index} but the snapshot ends at {snapshot_at} — history has a hole, refusing to start"
            ),
            StoreError::Decode(m) => write!(f, "decode error: {m}"),
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

#[derive(Debug)]
pub enum ClientError {
    /// This node is not the leader. The hint is the leader it last heard from,
    /// which may itself be stale — the client retries, it does not trust it.
    NotLeader {
        hint: Option<NodeId>,
    },
    /// The entry was appended but leadership was lost before it committed, so
    /// whether it survives is genuinely unknown until the client re-reads.
    LostLeadership,
    /// A linearizable read could not confirm leadership with a majority. The
    /// visible symptom of being on the minority side of a partition.
    ReadTimeout,
    Timeout,
    Transport(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::NotLeader { hint: Some(n) } => write!(f, "not leader (try node {n})"),
            ClientError::NotLeader { hint: None } => write!(f, "not leader (no leader known)"),
            ClientError::LostLeadership => {
                write!(
                    f,
                    "leadership lost before commit — outcome unknown, re-read"
                )
            }
            ClientError::ReadTimeout => {
                write!(f, "could not confirm leadership with a majority")
            }
            ClientError::Timeout => write!(f, "timed out"),
            ClientError::Transport(m) => write!(f, "transport: {m}"),
        }
    }
}
