//! Raft's state, split the way the paper splits it: the part that must survive a
//! crash, and the part that is safe to rebuild.
//!
//! Getting that split wrong is the classic bug. `current_term` and `voted_for`
//! are persistent because a node that forgets its vote can vote twice in one
//! term and elect two leaders. `commit_index` is *not* persistent because it is
//! safely recovered from the leader — rebuilding it is free, so storing it would
//! only add a way to be wrong.

use crate::proto::Reply;
use crate::types::{LogIndex, NodeId, StoreError, Term};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    pub fn label(&self) -> &'static str {
        match self {
            Role::Follower => "FOLLOWER",
            Role::Candidate => "CANDIDATE",
            Role::Leader => "LEADER",
        }
    }
}

/// Written with fsync before any reply that depends on it goes out.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Persistent {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

impl Persistent {
    pub fn load(path: &Path) -> Result<Persistent, StoreError> {
        match std::fs::read(path) {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| StoreError::Decode(format!("{e}")))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Persistent::default()),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Temp file, fsync, rename. A rename is atomic on both NTFS and ext4, so a
    /// crash leaves either the old state or the new one — never half of either.
    pub fn save(&self, path: &Path) -> Result<(), StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("state.tmp");
        let bytes = serde_json::to_vec(self).expect("persistent state is serialisable");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// A client write parked until its entry commits and applies.
pub struct Waiter {
    /// The term the entry was appended in. If the entry at this index later
    /// carries a different term, a different command took the slot and this
    /// client's write is gone.
    pub term: Term,
    pub started: Instant,
    pub tx: oneshot::Sender<Reply>,
}

/// A linearizable read parked until leadership is confirmed.
pub struct PendingRead {
    pub key: String,
    /// The commit index at the moment the read arrived. The answer must reflect
    /// at least this much of the log.
    pub read_index: LogIndex,
    /// The heartbeat round opened for this read. Only replies carrying this
    /// round or later prove leadership *now*; an older reply was already in
    /// flight and says nothing about the present.
    pub round: u64,
    pub registered_at: Instant,
    pub acks: HashSet<NodeId>,
    pub tx: oneshot::Sender<Reply>,
}

pub struct RaftState {
    pub role: Role,
    pub persistent: Persistent,
    /// Who this node currently believes is leader. A hint for clients, and
    /// routinely stale — which is why clients retry rather than trust it.
    pub leader_id: Option<NodeId>,

    pub commit_index: LogIndex,

    /// Candidate only: who has voted for us this term.
    pub votes: HashSet<NodeId>,

    /// Leader only: the next entry to send each follower, and the highest entry
    /// known to be replicated there. `next_index` is a guess that walks back on
    /// rejection; `match_index` is proven, and only it may drive commits.
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
    /// Last time each peer answered anything. Drives both the reachability shown
    /// in `status` and the majority check behind ReadIndex.
    pub last_ack: HashMap<NodeId, Instant>,

    pub waiters: HashMap<LogIndex, Waiter>,
    pub pending_reads: Vec<PendingRead>,
}

impl RaftState {
    pub fn new(persistent: Persistent) -> Self {
        RaftState {
            role: Role::Follower,
            persistent,
            leader_id: None,
            commit_index: 0,
            votes: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_ack: HashMap::new(),
            waiters: HashMap::new(),
            pending_reads: Vec::new(),
        }
    }

    pub fn term(&self) -> Term {
        self.persistent.current_term
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Leader bookkeeping is rebuilt from scratch on election. `next_index`
    /// starts optimistically at the end of the leader's own log and walks back
    /// until the follower agrees.
    pub fn init_leader_indexes(&mut self, peers: &[NodeId], last_log_index: LogIndex) {
        self.next_index.clear();
        self.match_index.clear();
        for peer in peers {
            self.next_index.insert(*peer, last_log_index + 1);
            self.match_index.insert(*peer, 0);
        }
    }

    /// The highest index replicated on a majority. Note this counts the leader
    /// itself — it has every entry it proposed, and it is part of the majority.
    pub fn majority_match(
        &self,
        peers: &[NodeId],
        last_log_index: LogIndex,
        majority: usize,
    ) -> LogIndex {
        let mut indexes: Vec<LogIndex> = peers
            .iter()
            .map(|p| self.match_index.get(p).copied().unwrap_or(0))
            .collect();
        indexes.push(last_log_index);
        indexes.sort_unstable();
        // Descending, take the majority-th: the highest index that at least
        // `majority` nodes have.
        indexes.reverse();
        indexes[majority - 1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> RaftState {
        RaftState::new(Persistent::default())
    }

    #[test]
    fn a_saved_vote_is_still_there_after_a_restart() {
        let dir = std::env::temp_dir().join(format!("dkv-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("raft.state");
        let p = Persistent {
            current_term: 7,
            voted_for: Some(2),
        };
        p.save(&path).expect("save");
        let back = Persistent::load(&path).expect("load");
        assert_eq!(back.current_term, 7);
        assert_eq!(back.voted_for, Some(2));
    }

    #[test]
    fn a_node_that_has_never_run_starts_at_term_zero_with_no_vote() {
        let path = std::env::temp_dir().join("dkv-nonexistent-state-file.json");
        let _ = std::fs::remove_file(&path);
        let p = Persistent::load(&path).expect("missing file is not an error");
        assert_eq!(p.current_term, 0);
        assert_eq!(p.voted_for, None);
    }

    #[test]
    fn commit_index_advances_only_to_what_a_majority_actually_holds() {
        let mut s = state();
        let peers = [2, 3];
        // The leader holds up to index 5; one follower has 5, the other has 2.
        s.match_index.insert(2, 5);
        s.match_index.insert(3, 2);
        assert_eq!(
            s.majority_match(&peers, 5, 2),
            5,
            "leader plus one follower is a majority at index 5"
        );

        // Now only the lagging follower has answered.
        s.match_index.insert(2, 1);
        assert_eq!(
            s.majority_match(&peers, 5, 2),
            2,
            "index 5 is only on the leader, which is not a majority"
        );
    }

    #[test]
    fn a_leader_alone_in_a_minority_cannot_advance_its_commit_index() {
        let mut s = state();
        let peers = [2, 3];
        s.match_index.insert(2, 0);
        s.match_index.insert(3, 0);
        assert_eq!(
            s.majority_match(&peers, 9, 2),
            0,
            "a partitioned leader may append forever and commit nothing"
        );
    }

    #[test]
    fn leader_bookkeeping_starts_optimistic_and_unproven() {
        let mut s = state();
        s.init_leader_indexes(&[2, 3], 12);
        assert_eq!(s.next_index[&2], 13, "assume the follower is caught up");
        assert_eq!(s.match_index[&2], 0, "but nothing is proven yet");
    }
}
