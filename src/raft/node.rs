//! The consensus loop. All of Raft's Figure 2 lives in this one file.
//!
//! It is one file on purpose. Split across `election.rs` and `replication.rs`,
//! the rule that matters most — *any* message carrying a higher term makes you a
//! follower — ends up written twice, and the two copies drift. Here it is
//! written once, at the top of `handle_message`, and nothing can route around it.
//!
//! One task owns every piece of consensus state. No `Arc<Mutex<RaftState>>`,
//! no locks to reason about: if you are reading this function, you are the only
//! writer. Everything slow happens somewhere else — peer sockets in their own
//! tasks, snapshot writes on the blocking pool — and reports back as an event.

use crate::config::Config;
use crate::faults::Faults;
use crate::logfmt::{Cat, Logger};
use crate::proto::{
    AdminCmd, AdminReply, ErrorKind, FaultStatus, Message, NodeStatus, Op, PeerStatus, Reply,
};
use crate::raft::log_store::LogStore;
use crate::raft::snapshot::{self, StoredSnapshot};
use crate::raft::state::{PendingRead, Persistent, RaftState, Role, Waiter};
use crate::statemachine::StateMachine;
use crate::transport::NodeEvent;
use crate::types::{ApplyResult, Command, LogEntry, LogIndex, NodeId, StoreError, Term};
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;

/// How many entries one AppendEntries may carry. Bounded so a follower that is
/// thousands of entries behind catches up over several rounds instead of in one
/// enormous frame.
const MAX_ENTRIES_PER_APPEND: usize = 64;

/// How long a client write waits before being told the outcome is unknown. It
/// only expires when the entry neither commits nor gets truncated — the
/// signature of being stuck on the minority side of a partition.
const WAITER_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a linearizable read waits for a majority to confirm leadership.
const READ_TIMEOUT: Duration = Duration::from_secs(3);

pub struct RaftNode {
    cfg: Config,
    logger: Arc<Logger>,
    state: RaftState,
    log: LogStore,
    sm: StateMachine,
    peers: HashMap<NodeId, mpsc::Sender<Message>>,
    faults: Arc<Mutex<Faults>>,
    events_rx: mpsc::Receiver<NodeEvent>,
    events_tx: mpsc::Sender<NodeEvent>,
    election_deadline: Instant,
    /// Edge-triggered peer health: only transitions get logged, so a steady
    /// cluster prints nothing instead of a line every heartbeat.
    reachable: HashMap<NodeId, bool>,
    read_round: u64,
    snapshot_in_flight: bool,
    snapshot_bytes: u64,
    /// When each peer was last sent a snapshot, so one being installed is not
    /// sent again on every rejected AppendEntries in the meantime.
    snapshot_sent_at: HashMap<NodeId, Instant>,
}

impl RaftNode {
    /// Loads whatever this node had on disk and returns it ready to run. Any
    /// state on disk is authoritative: a restarted node must not forget the term
    /// it was in or who it voted for.
    pub fn open(
        cfg: Config,
        logger: Arc<Logger>,
        faults: Arc<Mutex<Faults>>,
        events_tx: mpsc::Sender<NodeEvent>,
        events_rx: mpsc::Receiver<NodeEvent>,
        peers: HashMap<NodeId, mpsc::Sender<Message>>,
    ) -> Result<RaftNode, StoreError> {
        std::fs::create_dir_all(cfg.node_dir())?;
        let persistent = Persistent::load(&cfg.state_path())?;

        let mut sm = StateMachine::new();
        let stored = snapshot::load(&cfg.snapshot_path())?;
        let (snap_index, snap_term) = match stored {
            Some(s) => {
                let (i, t) = (s.last_included_index, s.last_included_term);
                sm.restore(s.payload, i);
                logger.say(
                    Cat::Snap,
                    format!(
                        "restored snapshot through idx {i} term {t} ({} keys)",
                        sm.keys()
                    ),
                );
                (i, t)
            }
            None => (0, 0),
        };

        let (log, report) = LogStore::open(&cfg.log_path(), cfg.fsync, snap_index, snap_term)?;
        if report.entries_loaded > 0 || report.truncated_at.is_some() {
            logger.say(
                Cat::Disk,
                format!(
                    "replayed {} entries (idx {}..{}), term {}",
                    report.entries_loaded,
                    log.first_index().min(log.last_index()),
                    log.last_index(),
                    persistent.current_term
                ),
            );
        }
        if let Some(at) = report.truncated_at {
            logger.say(
                Cat::Disk,
                format!(
                    "torn tail at byte {at}: dropped {} unfinished bytes — that write was never acknowledged",
                    report.bytes_dropped
                ),
            );
        }
        if report.pre_snapshot_skipped > 0 {
            logger.say(
                Cat::Disk,
                format!(
                    "finished an interrupted compaction: {} entries already in the snapshot",
                    report.pre_snapshot_skipped
                ),
            );
        }

        let mut state = RaftState::new(persistent);
        // Everything the snapshot covers is by definition committed.
        state.commit_index = snap_index;

        let mut node = RaftNode {
            snapshot_bytes: snapshot::size_bytes(&cfg.snapshot_path()),
            cfg,
            logger,
            state,
            log,
            sm,
            peers,
            faults,
            events_rx,
            events_tx,
            election_deadline: Instant::now(),
            reachable: HashMap::new(),
            read_round: 0,
            snapshot_in_flight: false,
            snapshot_sent_at: HashMap::new(),
        };
        node.reset_election_timer();
        Ok(node)
    }

    pub async fn run(mut self) {
        let mut heartbeat = tokio::time::interval(Duration::from_millis(self.cfg.heartbeat_ms));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        self.logger.say(
            Cat::Rcvr,
            format!(
                "node {} up — term {}, log idx {}..{}, {} keys, fsync={}",
                self.cfg.id,
                self.state.term(),
                self.log.first_index(),
                self.log.last_index(),
                self.sm.keys(),
                self.cfg.fsync.label()
            ),
        );

        loop {
            let deadline = tokio::time::Instant::from_std(self.election_deadline);
            tokio::select! {
                _ = heartbeat.tick() => {
                    if self.state.is_leader() {
                        self.broadcast_append();
                    }
                    self.check_peer_silence();
                    self.expire_waiters();
                    self.expire_reads();
                }
                _ = tokio::time::sleep_until(deadline) => {
                    if self.state.is_leader() {
                        self.reset_election_timer();
                    } else {
                        self.start_election();
                    }
                }
                event = self.events_rx.recv() => {
                    match event {
                        Some(event) => self.handle_event(event),
                        None => break,
                    }
                }
            }
        }
    }

    // ---- events ----------------------------------------------------------

    fn handle_event(&mut self, event: NodeEvent) {
        match event {
            NodeEvent::Incoming { msg, reply } => {
                // Hearing anything at all from a peer is proof it is alive. This
                // is how a follower tracks its leader: followers send nothing,
                // so inbound traffic is the only evidence they get.
                if let Some(sender) = msg.sender() {
                    self.note_ack(sender);
                }
                self.handle_message(msg, Some(reply));
            }
            NodeEvent::FromPeer { peer, msg } => {
                self.note_ack(peer);
                self.handle_message(msg, None);
            }
            NodeEvent::PeerUp(peer) => self.set_reachable(peer, true, "connected"),
            NodeEvent::PeerDown(peer) => self.set_reachable(peer, false, "connection lost"),
            NodeEvent::SnapshotWritten {
                last_included_index,
                bytes,
                error,
            } => self.on_snapshot_written(last_included_index, bytes, error),
        }
    }

    fn handle_message(&mut self, msg: Message, reply: Option<mpsc::Sender<Message>>) {
        // THE rule. Every Raft message carries a term, and seeing a higher one
        // means the world moved on without us — whatever we thought we were, we
        // are a follower now. Written once, here, so no handler can skip it.
        if let Some(term) = msg.term()
            && term > self.state.term()
        {
            let leader = match &msg {
                Message::AppendEntries { leader_id, .. }
                | Message::InstallSnapshot { leader_id, .. } => Some(*leader_id),
                _ => None,
            };
            self.step_down(term, leader, "saw a higher term");
        }

        match msg {
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(term, candidate_id, last_log_index, last_log_term, reply),
            Message::RequestVoteReply {
                term,
                from,
                vote_granted,
            } => self.on_vote_reply(term, from, vote_granted),
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
            } => self.on_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
                reply,
            ),
            Message::AppendEntriesReply {
                term,
                from,
                success,
                read_round,
                match_index,
                conflict_index,
                conflict_term,
            } => self.on_append_reply(
                term,
                from,
                success,
                read_round,
                match_index,
                conflict_index,
                conflict_term,
            ),
            Message::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                snapshot,
            } => self.on_install_snapshot(
                term,
                leader_id,
                last_included_index,
                last_included_term,
                snapshot,
                reply,
            ),
            Message::InstallSnapshotReply {
                term,
                from,
                last_included_index,
            } => self.on_install_snapshot_reply(term, from, last_included_index),
            Message::Request { client_id, seq, op } => {
                self.on_client_request(client_id, seq, op, reply)
            }
            Message::Admin(cmd) => self.on_admin(cmd, reply),
            Message::Reply(_) | Message::AdminReply(_) => {}
        }
    }

    // ---- elections -------------------------------------------------------

    fn reset_election_timer(&mut self) {
        let span = rand::rng().random_range(self.cfg.election_min_ms..=self.cfg.election_max_ms);
        self.election_deadline = Instant::now() + Duration::from_millis(span);
    }

    fn start_election(&mut self) {
        let silence = self.cfg.election_min_ms;
        self.logger.say(
            Cat::Elec,
            format!("no leader heard from in ~{silence}ms — starting an election"),
        );

        let was = self.state.role;
        self.state.persistent.current_term += 1;
        self.state.persistent.voted_for = Some(self.cfg.id);
        // Persist before campaigning. A node that crashes after voting and
        // forgets it can vote again in the same term, and two leaders in one
        // term is how a committed write gets overwritten.
        self.persist();

        self.state.role = Role::Candidate;
        self.state.leader_id = None;
        self.state.votes.clear();
        self.state.votes.insert(self.cfg.id);
        self.reset_election_timer();

        let term = self.state.term();
        self.logger
            .role_change(was.label(), "CANDIDATE", term, "voting for itself");

        let msg = Message::RequestVote {
            term,
            candidate_id: self.cfg.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for peer in self.cfg.peers() {
            self.send_to(peer, msg.clone());
        }
        self.logger.say(
            Cat::Elec,
            format!(
                "RequestVote term {term} lastIdx {} lastTerm {} -> {}",
                self.log.last_index(),
                self.log.last_term(),
                self.peer_list()
            ),
        );
    }

    fn on_request_vote(
        &mut self,
        term: Term,
        candidate: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
        reply: Option<mpsc::Sender<Message>>,
    ) {
        let my_term = self.state.term();
        let already_voted = self.state.persistent.voted_for;

        // The candidate's log must be at least as up to date as ours: later term
        // wins, and at equal terms the longer log wins. Skipping this check is
        // how a node with a truncated log gets elected and erases committed
        // writes — the single most damaging bug in a hand-rolled Raft.
        let up_to_date =
            (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index());
        let free_to_vote = already_voted.is_none() || already_voted == Some(candidate);
        let granted = term >= my_term && free_to_vote && up_to_date;

        if granted {
            self.state.persistent.voted_for = Some(candidate);
            self.persist();
            // Granting a vote is one of exactly two things that may reset the
            // timer. Resetting on anything else lets a stale leader keep a
            // follower from ever campaigning.
            self.reset_election_timer();
            self.logger.say(
                Cat::Elec,
                format!("granted vote to n{candidate} (term {term})"),
            );
        } else {
            let why = if term < my_term {
                format!("its term {term} is behind ours {my_term}")
            } else if !free_to_vote {
                format!(
                    "already voted for n{} this term",
                    already_voted.unwrap_or(0)
                )
            } else {
                format!(
                    "its log ({last_log_term}/{last_log_index}) is behind ours ({}/{})",
                    self.log.last_term(),
                    self.log.last_index()
                )
            };
            self.logger
                .say(Cat::Elec, format!("refused vote to n{candidate}: {why}"));
        }

        self.respond(
            reply,
            Message::RequestVoteReply {
                term: self.state.term(),
                from: self.cfg.id,
                vote_granted: granted,
            },
        );
    }

    fn on_vote_reply(&mut self, term: Term, from: NodeId, granted: bool) {
        if self.state.role != Role::Candidate || term != self.state.term() {
            return;
        }
        if !granted {
            return;
        }
        self.state.votes.insert(from);
        let votes = self.state.votes.len();
        let needed = self.cfg.majority();
        self.logger.say(
            Cat::Elec,
            format!("vote from n{from} ({votes}/{needed} needed)"),
        );
        if votes >= needed {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        let term = self.state.term();
        let votes = self.state.votes.len();
        self.state.role = Role::Leader;
        self.state.leader_id = Some(self.cfg.id);
        self.state
            .init_leader_indexes(&self.cfg.peers(), self.log.last_index());
        self.logger.role_change(
            "CANDIDATE",
            "LEADER",
            term,
            &format!("{votes}/{} votes", crate::config::CLUSTER.len()),
        );

        // A fresh leader appends a no-op in its own term. Until an entry from
        // the current term commits, the Figure 8 rule forbids committing
        // anything inherited from older terms — so without this, entries from a
        // previous leader could sit replicated but uncommittable indefinitely.
        let entry = LogEntry {
            index: self.log.last_index() + 1,
            term,
            client_id: 0,
            seq: 0,
            command: Command::Noop,
        };
        if let Err(e) = self.log.append(&[entry]) {
            self.logger
                .say(Cat::Fail, format!("cannot append noop: {e}"));
        }
        self.broadcast_append();
    }

    fn step_down(&mut self, term: Term, leader: Option<NodeId>, why: &str) {
        let was = self.state.role;
        if term > self.state.term() {
            self.state.persistent.current_term = term;
            self.state.persistent.voted_for = None;
            self.persist();
        }
        self.state.role = Role::Follower;
        self.state.leader_id = leader;
        self.state.votes.clear();
        if was != Role::Follower {
            self.logger.role_change(was.label(), "FOLLOWER", term, why);
            // Reads were waiting on a majority that will never answer to us now.
            self.fail_reads(ErrorKind::NotLeader, "no longer leader");
        }
    }

    // ---- replication -----------------------------------------------------

    fn broadcast_append(&mut self) {
        for peer in self.cfg.peers() {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let next = self.state.next_index.get(&peer).copied().unwrap_or(1);

        // The entries this follower needs are already compacted away, so the
        // only way to catch it up is to ship the whole state machine.
        if next <= self.log.snapshot_index() {
            self.send_snapshot(peer);
            return;
        }
        let prev_index = next.saturating_sub(1);
        let Some(prev_term) = self.log.term_at(prev_index) else {
            self.send_snapshot(peer);
            return;
        };

        let entries = self.log.entries_from(next, MAX_ENTRIES_PER_APPEND);
        if !entries.is_empty() {
            self.logger.detail(
                Cat::Repl,
                format!(
                    "append idx {}..{} -> n{peer} (prev {prev_index}/{prev_term})",
                    entries[0].index,
                    entries[entries.len() - 1].index
                ),
            );
        } else {
            self.logger.detail(
                Cat::Beat,
                format!("heartbeat -> n{peer} (prev {prev_index})"),
            );
        }

        let msg = Message::AppendEntries {
            term: self.state.term(),
            leader_id: self.cfg.id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.state.commit_index,
            read_round: self.read_round,
        };
        self.send_to(peer, msg);
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        term: Term,
        leader: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        read_round: u64,
        reply: Option<mpsc::Sender<Message>>,
    ) {
        let my_term = self.state.term();
        if term < my_term {
            // A leader from the past. Answering with our term is what tells it
            // to step down.
            self.respond(
                reply,
                Message::AppendEntriesReply {
                    term: my_term,
                    from: self.cfg.id,
                    success: false,
                    read_round,
                    match_index: 0,
                    conflict_index: 0,
                    conflict_term: 0,
                },
            );
            return;
        }

        // A valid AppendEntries from the current leader: the other of the two
        // things that may reset the election timer.
        self.reset_election_timer();
        if self.state.role != Role::Follower {
            self.step_down(term, Some(leader), &format!("n{leader} is leader"));
        }
        if self.state.leader_id != Some(leader) {
            self.state.leader_id = Some(leader);
            self.logger
                .say(Cat::Rcvr, format!("following n{leader} (term {term})"));
        }

        // Log matching: refuse unless the entry before the ones being sent is
        // the same entry we have. The conflict hints let the leader jump back a
        // whole term at a time instead of one index per round trip.
        let matches = match self.log.term_at(prev_log_index) {
            Some(t) => t == prev_log_term,
            None => prev_log_index == 0,
        };
        if !matches {
            let (conflict_index, conflict_term) = if prev_log_index > self.log.last_index() {
                (self.log.last_index() + 1, 0)
            } else if prev_log_index < self.log.snapshot_index() {
                (self.log.snapshot_index() + 1, 0)
            } else {
                let t = self.log.term_at(prev_log_index).unwrap_or(0);
                (self.first_index_with_term(t), t)
            };
            self.logger.say(
                Cat::Repl,
                format!(
                    "rejected append from n{leader}: prev {prev_log_index}/{prev_log_term} does not match ours — asking it to resend from {conflict_index}"
                ),
            );
            self.respond(
                reply,
                Message::AppendEntriesReply {
                    term: my_term,
                    from: self.cfg.id,
                    success: false,
                    read_round,
                    match_index: 0,
                    conflict_index,
                    conflict_term,
                },
            );
            return;
        }

        let last_new = prev_log_index + entries.len() as LogIndex;
        self.append_from_leader(entries);

        // `min` matters: the leader's commit index may be ahead of what it has
        // actually sent us yet.
        if leader_commit > self.state.commit_index {
            let new_commit = leader_commit.min(self.log.last_index());
            if new_commit > self.state.commit_index {
                self.state.commit_index = new_commit;
                self.apply_committed();
            }
        }

        self.respond(
            reply,
            Message::AppendEntriesReply {
                term: self.state.term(),
                from: self.cfg.id,
                success: true,
                read_round,
                match_index: last_new,
                conflict_index: 0,
                conflict_term: 0,
            },
        );
    }

    /// Writes the leader's entries into our log, truncating **only** where the
    /// terms actually disagree.
    ///
    /// The tempting version — truncate everything from `prev_log_index + 1` and
    /// append — is wrong. A duplicate or reordered AppendEntries that the
    /// network delayed would then delete entries the leader has since sent and
    /// believes we hold, and the follower silently loses data it already acked.
    fn append_from_leader(&mut self, entries: Vec<LogEntry>) {
        let mut to_append: Vec<LogEntry> = Vec::new();
        for entry in entries {
            if !to_append.is_empty() {
                to_append.push(entry);
                continue;
            }
            match self.log.term_at(entry.index) {
                Some(existing) if existing == entry.term => continue, // already have it
                Some(_) => {
                    match self.log.truncate_from(entry.index) {
                        Ok(dropped) if dropped > 0 => self.logger.say(
                            Cat::Repl,
                            format!(
                                "conflict at idx {}: dropped {dropped} of our entries — the leader's log wins",
                                entry.index
                            ),
                        ),
                        Ok(_) => {}
                        Err(e) => self.logger.say(Cat::Fail, format!("truncate failed: {e}")),
                    }
                    // Those entries are gone; anyone still waiting on them has
                    // lost the write.
                    self.fail_orphaned_waiters();
                    to_append.push(entry);
                }
                None => to_append.push(entry),
            }
        }
        if !to_append.is_empty()
            && let Err(e) = self.log.append(&to_append)
        {
            self.logger.say(Cat::Fail, format!("append failed: {e}"));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_reply(
        &mut self,
        term: Term,
        from: NodeId,
        success: bool,
        read_round: u64,
        match_index: LogIndex,
        conflict_index: LogIndex,
        conflict_term: Term,
    ) {
        // A reply from an older term is an echo of a conversation that no longer
        // matters.
        if !self.state.is_leader() || term != self.state.term() {
            return;
        }
        self.note_ack(from);

        if success {
            let prev = self.state.match_index.get(&from).copied().unwrap_or(0);
            let now = prev.max(match_index);
            self.state.match_index.insert(from, now);
            self.state.next_index.insert(from, now + 1);
            if now > prev {
                self.logger
                    .detail(Cat::Repl, format!("n{from} acked through idx {now}"));
            }
            self.count_read_ack(from, read_round);
            // Must happen whether or not the commit index moved: a heartbeat
            // round opened purely to confirm leadership for a read commits
            // nothing, so waiting for `apply_committed` to run would leave every
            // read hanging until it timed out.
            self.serve_ready_reads();
            self.maybe_advance_commit();
            // Still behind: keep going immediately rather than waiting for the
            // next heartbeat.
            if self.state.next_index[&from] <= self.log.last_index() {
                self.send_append(from);
            }
            return;
        }

        // Rejected: walk this follower's next_index back. Using the conflicting
        // term skips the entire bad stretch in one step.
        let next = if conflict_term > 0 {
            match self.last_index_with_term(conflict_term) {
                Some(i) => i + 1,
                None => conflict_index.max(1),
            }
        } else {
            conflict_index.max(1)
        };
        self.state.next_index.insert(from, next);
        self.logger.detail(
            Cat::Repl,
            format!("n{from} rejected — retrying from idx {next}"),
        );
        self.send_append(from);
    }

    fn maybe_advance_commit(&mut self) {
        let peers = self.cfg.peers();
        let candidate =
            self.state
                .majority_match(&peers, self.log.last_index(), self.cfg.majority());
        if candidate <= self.state.commit_index {
            return;
        }
        // Figure 8. A leader may only count replicas for an entry from its
        // *own* term. An older entry that merely looks well replicated can still
        // be overwritten by a future leader, and committing it here would make a
        // committed value change later — the one thing Raft promises never
        // happens. Older entries commit implicitly, carried along by this one.
        if self.log.term_at(candidate) != Some(self.state.term()) {
            self.logger.detail(
                Cat::Repl,
                format!(
                    "idx {candidate} is replicated on a majority but belongs to term {:?}, not {} — holding",
                    self.log.term_at(candidate),
                    self.state.term()
                ),
            );
            return;
        }
        self.state.commit_index = candidate;
        self.apply_committed();
    }

    // ---- applying --------------------------------------------------------

    fn apply_committed(&mut self) {
        while self.sm.last_applied < self.state.commit_index {
            let index = self.sm.last_applied + 1;
            let Some(entry) = self.log.entry(index).cloned() else {
                break;
            };
            let applied = self.sm.apply(&entry);

            if let Some(waiter) = self.state.waiters.remove(&index) {
                let elapsed = waiter.started.elapsed().as_millis() as u64;
                if waiter.term == entry.term {
                    self.logger.say(
                        Cat::Clnt,
                        format!(
                            "committed idx {index} {} (quorum {}/{}, {elapsed}ms){}",
                            entry.command.summary(),
                            self.cfg.majority(),
                            crate::config::CLUSTER.len(),
                            if applied.duplicate {
                                " [duplicate, not reapplied]"
                            } else {
                                ""
                            }
                        ),
                    );
                    let _ = waiter.tx.send(Reply::Committed {
                        index,
                        term: entry.term,
                        ms: elapsed,
                        duplicate: applied.duplicate,
                    });
                } else {
                    // A different command took this slot.
                    let _ = waiter.tx.send(Reply::Error {
                        kind: ErrorKind::LostLeadership,
                        message: "another leader's entry took this index".into(),
                        leader_hint: self.state.leader_id,
                    });
                }
            } else if !entry.is_internal() {
                self.logger.say(
                    Cat::Repl,
                    format!("applied idx {index} {}", entry.command.summary()),
                );
            }
        }
        self.serve_ready_reads();
        self.maybe_snapshot();
    }

    // ---- client requests -------------------------------------------------

    fn on_client_request(
        &mut self,
        client_id: u64,
        seq: u64,
        op: Op,
        reply: Option<mpsc::Sender<Message>>,
    ) {
        match op {
            Op::Get { key, stale: true } => {
                // No leadership check at all. Fast, local, and possibly behind —
                // exactly what the `--stale` flag is for.
                let value = self.sm.get(&key);
                self.logger.say(
                    Cat::Clnt,
                    format!(
                        "GET {key} (stale) -> {} @ applied idx {}",
                        value.clone().unwrap_or_else(|| "<none>".into()),
                        self.sm.last_applied
                    ),
                );
                self.respond(
                    reply,
                    Message::Reply(Reply::Value {
                        value,
                        index: self.sm.last_applied,
                        stale: true,
                    }),
                );
            }
            Op::Get { key, stale: false } => self.start_linearizable_read(key, reply),
            Op::Set { key, value } => {
                self.propose(client_id, seq, Command::Set { key, value }, reply)
            }
            Op::Delete { key } => self.propose(client_id, seq, Command::Delete { key }, reply),
        }
    }

    fn propose(
        &mut self,
        client_id: u64,
        seq: u64,
        command: Command,
        reply: Option<mpsc::Sender<Message>>,
    ) {
        if !self.state.is_leader() {
            self.reject_not_leader(reply);
            return;
        }
        let index = self.log.last_index() + 1;
        let term = self.state.term();
        let entry = LogEntry {
            index,
            term,
            client_id,
            seq,
            command: command.clone(),
        };

        // Append and fsync *before* anybody is told anything. A write we have
        // acknowledged but not persisted is a write we can lose in a crash while
        // the client believes it is safe.
        if let Err(e) = self.log.append(&[entry]) {
            self.logger.say(Cat::Fail, format!("append failed: {e}"));
            self.respond(
                reply,
                Message::Reply(Reply::Error {
                    kind: ErrorKind::Internal,
                    message: format!("{e}"),
                    leader_hint: Some(self.cfg.id),
                }),
            );
            return;
        }

        self.logger.say(
            Cat::Clnt,
            format!("{} -> log idx {index} term {term}", command.summary()),
        );

        if let Some(tx) = reply {
            let (done_tx, done_rx) = oneshot::channel();
            self.state.waiters.insert(
                index,
                Waiter {
                    term,
                    started: Instant::now(),
                    tx: done_tx,
                },
            );
            // The reply is sent from a tiny task so the consensus loop never
            // waits on a client socket.
            tokio::spawn(async move {
                if let Ok(result) = done_rx.await {
                    let _ = tx.send(Message::Reply(result)).await;
                }
            });
        }

        self.broadcast_append();
    }

    fn start_linearizable_read(&mut self, key: String, reply: Option<mpsc::Sender<Message>>) {
        if !self.state.is_leader() {
            self.reject_not_leader(reply);
            return;
        }
        let Some(tx) = reply else { return };
        let (done_tx, done_rx) = oneshot::channel();
        tokio::spawn(async move {
            if let Ok(result) = done_rx.await {
                let _ = tx.send(Message::Reply(result)).await;
            }
        });

        // ReadIndex. Being leader is not enough to answer a read: this node may
        // have been deposed seconds ago and not heard about it yet. So pin the
        // current commit index, prove leadership with a fresh heartbeat round,
        // and only then answer.
        self.read_round += 1;
        let round = self.read_round;
        let read_index = self.state.commit_index;
        self.state.pending_reads.push(PendingRead {
            key: key.clone(),
            read_index,
            round,
            registered_at: Instant::now(),
            acks: std::collections::HashSet::new(),
            tx: done_tx,
        });
        self.logger.detail(
            Cat::Clnt,
            format!("GET {key}: confirming leadership at commit idx {read_index} (round {round})"),
        );
        self.broadcast_append();
        // A single-node majority would already be satisfied; with three nodes
        // this only fires once peers answer.
        self.serve_ready_reads();
    }

    fn count_read_ack(&mut self, from: NodeId, round: u64) {
        for read in &mut self.state.pending_reads {
            if round >= read.round {
                read.acks.insert(from);
            }
        }
    }

    fn serve_ready_reads(&mut self) {
        if self.state.pending_reads.is_empty() {
            return;
        }
        let majority = self.cfg.majority();
        let applied = self.sm.last_applied;
        let mut still_pending = Vec::new();
        // `drain` rather than index juggling: each read is either answered now
        // or put back.
        for read in std::mem::take(&mut self.state.pending_reads) {
            // +1 for this node itself, which has obviously acked.
            let confirmed = read.acks.len() + 1 >= majority;
            if confirmed && applied >= read.read_index {
                let value = self.sm.get(&read.key);
                self.logger.say(
                    Cat::Clnt,
                    format!(
                        "GET {} -> {} (linearizable, confirmed by {}/{} at idx {})",
                        read.key,
                        value.clone().unwrap_or_else(|| "<none>".into()),
                        read.acks.len() + 1,
                        crate::config::CLUSTER.len(),
                        read.read_index
                    ),
                );
                let _ = read.tx.send(Reply::Value {
                    value,
                    index: read.read_index,
                    stale: false,
                });
            } else {
                still_pending.push(read);
            }
        }
        self.state.pending_reads = still_pending;
    }

    fn expire_reads(&mut self) {
        let timed_out: Vec<PendingRead> = {
            let (dead, alive): (Vec<PendingRead>, Vec<PendingRead>) =
                std::mem::take(&mut self.state.pending_reads)
                    .into_iter()
                    .partition(|r| r.registered_at.elapsed() > READ_TIMEOUT);
            self.state.pending_reads = alive;
            dead
        };
        for read in timed_out {
            self.logger.say(
                Cat::Fail,
                format!(
                    "GET {}: could not confirm leadership with a majority — this node may be on the minority side of a partition",
                    read.key
                ),
            );
            let _ = read.tx.send(Reply::Error {
                kind: ErrorKind::ReadTimeout,
                message: "no majority confirmed leadership".into(),
                leader_hint: None,
            });
        }
    }

    fn fail_reads(&mut self, kind: ErrorKind, message: &str) {
        for read in std::mem::take(&mut self.state.pending_reads) {
            let _ = read.tx.send(Reply::Error {
                kind,
                message: message.to_string(),
                leader_hint: self.state.leader_id,
            });
        }
    }

    /// Waiters whose entry is gone, or has been replaced by a different term at
    /// the same index. Without this the client hangs forever — or worse, is told
    /// its write committed when the entry now at that index belongs to somebody
    /// else's command.
    fn fail_orphaned_waiters(&mut self) {
        let last = self.log.last_index();
        let orphans: Vec<LogIndex> = self
            .state
            .waiters
            .iter()
            .filter(|(index, waiter)| {
                **index > last || self.log.term_at(**index) != Some(waiter.term)
            })
            .map(|(index, _)| *index)
            .collect();
        for index in orphans {
            if let Some(waiter) = self.state.waiters.remove(&index) {
                self.logger.say(
                    Cat::Fail,
                    format!("write at idx {index} was truncated — it never committed"),
                );
                let _ = waiter.tx.send(Reply::Error {
                    kind: ErrorKind::LostLeadership,
                    message: "entry was truncated by a newer leader".into(),
                    leader_hint: self.state.leader_id,
                });
            }
        }
    }

    fn expire_waiters(&mut self) {
        let stale: Vec<LogIndex> = self
            .state
            .waiters
            .iter()
            .filter(|(_, w)| w.started.elapsed() > WAITER_TIMEOUT)
            .map(|(i, _)| *i)
            .collect();
        for index in stale {
            if let Some(waiter) = self.state.waiters.remove(&index) {
                self.logger.say(
                    Cat::Fail,
                    format!(
                        "write at idx {index} has not committed in {}s — no majority is reachable",
                        WAITER_TIMEOUT.as_secs()
                    ),
                );
                let _ = waiter.tx.send(Reply::Error {
                    kind: ErrorKind::LostLeadership,
                    message: "no majority reachable; outcome unknown until this node rejoins"
                        .into(),
                    leader_hint: None,
                });
            }
        }
    }

    fn reject_not_leader(&mut self, reply: Option<mpsc::Sender<Message>>) {
        let hint = self.state.leader_id;
        self.respond(
            reply,
            Message::Reply(Reply::Error {
                kind: ErrorKind::NotLeader,
                message: match hint {
                    Some(n) => format!("not leader — n{n} was, last we heard"),
                    None => "not leader and no leader known".to_string(),
                },
                leader_hint: hint,
            }),
        );
    }

    // ---- snapshots -------------------------------------------------------

    fn maybe_snapshot(&mut self) {
        if self.snapshot_in_flight || (self.log.entry_count() as u64) <= self.cfg.snapshot_threshold
        {
            return;
        }
        let index = self.sm.last_applied;
        if index <= self.log.snapshot_index() {
            return;
        }
        let Some(term) = self.log.term_at(index) else {
            return;
        };

        // The payload is cloned here, synchronously, so it is a true picture of
        // index `index`. Only the file write goes to the blocking pool — the
        // consensus loop must not stall on a disk.
        let stored = StoredSnapshot {
            last_included_index: index,
            last_included_term: term,
            payload: self.sm.snapshot(),
        };
        let path = self.cfg.snapshot_path();
        let events = self.events_tx.clone();
        self.snapshot_in_flight = true;
        self.logger.say(
            Cat::Snap,
            format!(
                "log has {} entries — snapshotting state through idx {index}",
                self.log.entry_count()
            ),
        );
        tokio::task::spawn_blocking(move || {
            let (bytes, error) = match snapshot::save(&path, &stored) {
                Ok(bytes) => (bytes, None),
                Err(e) => (0, Some(format!("{e}"))),
            };
            let _ = events.blocking_send(NodeEvent::SnapshotWritten {
                last_included_index: index,
                bytes,
                error,
            });
        });
    }

    fn on_snapshot_written(&mut self, index: LogIndex, bytes: u64, error: Option<String>) {
        self.snapshot_in_flight = false;
        if let Some(e) = error {
            self.logger
                .say(Cat::Fail, format!("snapshot write failed: {e}"));
            return;
        }
        self.snapshot_bytes = bytes;
        let Some(term) = self.log.term_at(index) else {
            return;
        };
        // Only now, with the snapshot durable, may the log be truncated.
        match self.log.compact(index, term) {
            Ok(dropped) => self.logger.say(
                Cat::Snap,
                format!(
                    "snapshot durable ({bytes} bytes) — compacted {dropped} entries, log now starts at idx {}",
                    self.log.first_index()
                ),
            ),
            Err(e) => self.logger.say(Cat::Fail, format!("compaction failed: {e}")),
        }
    }

    fn send_snapshot(&mut self, peer: NodeId) {
        // A follower being caught up keeps rejecting AppendEntries until the
        // snapshot lands, and each rejection walks `next_index` back into
        // snapshot territory again. Without this throttle the leader ships the
        // whole state machine several times in the same millisecond.
        let in_flight = Duration::from_millis(self.cfg.heartbeat_ms * 2);
        if let Some(sent) = self.snapshot_sent_at.get(&peer)
            && sent.elapsed() < in_flight
        {
            return;
        }
        self.snapshot_sent_at.insert(peer, Instant::now());

        let index = self.log.snapshot_index();
        let term = self.log.snapshot_term();
        self.logger.say(
            Cat::Snap,
            format!(
                "n{peer} needs entries we compacted away — sending snapshot through idx {index}"
            ),
        );
        let msg = Message::InstallSnapshot {
            term: self.state.term(),
            leader_id: self.cfg.id,
            last_included_index: index,
            last_included_term: term,
            snapshot: self.sm.snapshot(),
        };
        self.send_to(peer, msg);
        // Assume it lands; a failure just means the next heartbeat tries again.
        self.state.next_index.insert(peer, index + 1);
    }

    fn on_install_snapshot(
        &mut self,
        term: Term,
        leader: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        payload: crate::proto::SnapshotPayload,
        reply: Option<mpsc::Sender<Message>>,
    ) {
        if term < self.state.term() {
            return;
        }
        self.reset_election_timer();
        self.state.leader_id = Some(leader);

        if last_included_index > self.sm.last_applied {
            self.sm.restore(payload, last_included_index);
            let stored = StoredSnapshot {
                last_included_index,
                last_included_term,
                payload: self.sm.snapshot(),
            };
            if let Err(e) = snapshot::save(&self.cfg.snapshot_path(), &stored) {
                self.logger
                    .say(Cat::Fail, format!("cannot store snapshot: {e}"));
            }
            if let Err(e) = self.log.compact(last_included_index, last_included_term) {
                self.logger
                    .say(Cat::Fail, format!("compaction failed: {e}"));
            }
            self.state.commit_index = self.state.commit_index.max(last_included_index);
            self.logger.say(
                Cat::Rcvr,
                format!(
                    "installed snapshot from n{leader} through idx {last_included_index} ({} keys) — caught up in one step",
                    self.sm.keys()
                ),
            );
        }

        self.respond(
            reply,
            Message::InstallSnapshotReply {
                term: self.state.term(),
                from: self.cfg.id,
                last_included_index,
            },
        );
    }

    fn on_install_snapshot_reply(&mut self, term: Term, from: NodeId, index: LogIndex) {
        if !self.state.is_leader() || term != self.state.term() {
            return;
        }
        self.note_ack(from);
        self.state.match_index.insert(from, index);
        self.state.next_index.insert(from, index + 1);
        self.send_append(from);
    }

    // ---- admin -----------------------------------------------------------

    fn on_admin(&mut self, cmd: AdminCmd, reply: Option<mpsc::Sender<Message>>) {
        let response = match cmd {
            AdminCmd::Status => AdminReply::Status(Box::new(self.status())),
            AdminCmd::InspectLog { from, to } => {
                let from = from.unwrap_or(self.log.first_index());
                let to = to.unwrap_or(self.log.last_index());
                let entries = self
                    .log
                    .all_entries()
                    .iter()
                    .filter(|e| e.index >= from && e.index <= to)
                    .cloned()
                    .collect();
                AdminReply::LogEntries {
                    entries,
                    first_index: self.log.first_index(),
                    last_index: self.log.last_index(),
                    commit_index: self.state.commit_index,
                }
            }
            AdminCmd::InspectState => AdminReply::State {
                kv: self.sm.kv().clone(),
                last_applied: self.sm.last_applied,
                state_hash: self.sm.state_hash(),
                clients: self.sm.clients(),
            },
            AdminCmd::InspectSnapshot => AdminReply::Snapshot {
                last_included_index: self.log.snapshot_index(),
                last_included_term: self.log.snapshot_term(),
                keys: self.sm.keys(),
                bytes: self.snapshot_bytes,
            },
            AdminCmd::Partition { peer, on } => {
                if let Ok(mut f) = self.faults.lock() {
                    f.partition(peer, on);
                }
                self.logger.say(
                    Cat::Admn,
                    if on {
                        format!("link to n{peer} CUT — messages both ways are dropped")
                    } else {
                        format!("link to n{peer} restored")
                    },
                );
                AdminReply::Ok
            }
            AdminCmd::Fault {
                peer,
                drop_rate,
                delay_ms,
                clear,
            } => {
                if let Ok(mut f) = self.faults.lock() {
                    match (clear, peer) {
                        (true, Some(p)) => f.clear_peer(p),
                        (true, None) => f.clear(),
                        (false, Some(p)) => f.set(p, drop_rate, delay_ms),
                        (false, None) => {
                            for p in self.cfg.peers() {
                                f.set(p, drop_rate, delay_ms);
                            }
                        }
                    }
                }
                self.logger.say(
                    Cat::Admn,
                    if clear {
                        "link faults cleared".to_string()
                    } else {
                        format!(
                            "link fault set: drop={} delay={}ms",
                            drop_rate.unwrap_or(0.0),
                            delay_ms.unwrap_or(0)
                        )
                    },
                );
                AdminReply::Ok
            }
            AdminCmd::Compact => {
                let index = self.sm.last_applied;
                match self.log.term_at(index) {
                    Some(term) => {
                        let stored = StoredSnapshot {
                            last_included_index: index,
                            last_included_term: term,
                            payload: self.sm.snapshot(),
                        };
                        match snapshot::save(&self.cfg.snapshot_path(), &stored) {
                            Ok(bytes) => {
                                self.snapshot_bytes = bytes;
                                match self.log.compact(index, term) {
                                    Ok(dropped) => {
                                        self.logger.say(
                                            Cat::Snap,
                                            format!(
                                                "compacted {dropped} entries through idx {index}"
                                            ),
                                        );
                                        AdminReply::Ok
                                    }
                                    Err(e) => AdminReply::Error {
                                        message: format!("{e}"),
                                    },
                                }
                            }
                            Err(e) => AdminReply::Error {
                                message: format!("{e}"),
                            },
                        }
                    }
                    None => AdminReply::Error {
                        message: "nothing new to snapshot".into(),
                    },
                }
            }
            AdminCmd::StepDown => {
                if self.state.is_leader() {
                    let term = self.state.term();
                    self.step_down(term, None, "told to step down");
                }
                AdminReply::Ok
            }
            AdminCmd::Campaign => {
                self.start_election();
                AdminReply::Ok
            }
        };
        self.respond(reply, Message::AdminReply(response));
    }

    fn status(&self) -> NodeStatus {
        let faults: Vec<FaultStatus> = self.faults.lock().map(|f| f.status()).unwrap_or_default();
        let peers = self
            .cfg
            .peers()
            .iter()
            .map(|p| PeerStatus {
                id: *p,
                reachable: self.reachable.get(p).copied().unwrap_or(false),
                last_ack_ms: self
                    .state
                    .last_ack
                    .get(p)
                    .map(|t| t.elapsed().as_millis() as u64),
                next_index: self.state.next_index.get(p).copied().unwrap_or(0),
                match_index: self.state.match_index.get(p).copied().unwrap_or(0),
            })
            .collect();
        NodeStatus {
            id: self.cfg.id,
            role: self.state.role.label().to_string(),
            term: self.state.term(),
            leader: self.state.leader_id,
            commit_index: self.state.commit_index,
            last_applied: self.sm.last_applied,
            first_index: self.log.first_index(),
            last_index: self.log.last_index(),
            log_entries: self.log.entry_count() as u64,
            keys: self.sm.keys(),
            state_hash: self.sm.state_hash(),
            snapshot_index: self.log.snapshot_index(),
            fsync: self.cfg.fsync.label().to_string(),
            peers,
            faults,
        }
    }

    // ---- helpers ---------------------------------------------------------

    fn persist(&mut self) {
        if let Err(e) = self.state.persistent.save(&self.cfg.state_path()) {
            self.logger
                .say(Cat::Fail, format!("cannot persist term/vote: {e}"));
        }
    }

    fn send_to(&self, peer: NodeId, msg: Message) {
        if let Some(tx) = self.peers.get(&peer) {
            // try_send, never send().await: the consensus loop must not block on
            // a peer that is slow or gone.
            let _ = tx.try_send(msg);
        }
    }

    fn respond(&self, reply: Option<mpsc::Sender<Message>>, msg: Message) {
        if let Some(tx) = reply {
            let _ = tx.try_send(msg);
        }
    }

    fn note_ack(&mut self, peer: NodeId) {
        self.state.last_ack.insert(peer, Instant::now());
        if !self.reachable.get(&peer).copied().unwrap_or(false) {
            self.set_reachable(peer, true, "answering again");
        }
    }

    /// Edge-triggered: one line when a peer goes quiet, one when it comes back.
    fn set_reachable(&mut self, peer: NodeId, up: bool, why: &str) {
        let was = self.reachable.insert(peer, up);
        if was == Some(up) {
            return;
        }
        if up {
            // Start this peer's silence clock now. Without it a freshly
            // connected peer looks like it has been quiet since the beginning of
            // time and gets declared dead on the very next tick.
            self.state.last_ack.entry(peer).or_insert_with(Instant::now);
            self.logger.say(
                Cat::Rcvr,
                match was {
                    None => format!("n{peer} connected"),
                    Some(_) => format!("n{peer} reachable again ({why})"),
                },
            );
            if self.state.is_leader() {
                self.send_append(peer);
            }
        } else {
            self.logger
                .say(Cat::Fail, format!("n{peer} unreachable ({why})"));
        }
    }

    /// A partitioned link keeps its TCP connection open, so silence — not a
    /// socket error — is what exposes it.
    ///
    /// Only the leader runs this. It is the one node that has a right to expect
    /// regular replies from everybody; a follower never hears from another
    /// follower even in a perfectly healthy cluster, so silence there would mean
    /// nothing and flagging it would be a lie.
    fn check_peer_silence(&mut self) {
        if !self.state.is_leader() {
            return;
        }
        let limit = Duration::from_millis(self.cfg.heartbeat_ms * 3);
        let quiet: Vec<NodeId> = self
            .cfg
            .peers()
            .iter()
            .copied()
            .filter(|p| {
                self.reachable.get(p).copied().unwrap_or(false)
                    && self
                        .state
                        .last_ack
                        .get(p)
                        .is_some_and(|t| t.elapsed() > limit)
            })
            .collect();
        for peer in quiet {
            let silent_for = self
                .state
                .last_ack
                .get(&peer)
                .map(|t| t.elapsed().as_millis())
                .unwrap_or(0);
            self.set_reachable(peer, false, &format!("silent for {silent_for}ms"));
        }
    }

    fn first_index_with_term(&self, term: Term) -> LogIndex {
        self.log
            .all_entries()
            .iter()
            .find(|e| e.term == term)
            .map(|e| e.index)
            .unwrap_or_else(|| self.log.first_index())
    }

    fn last_index_with_term(&self, term: Term) -> Option<LogIndex> {
        self.log
            .all_entries()
            .iter()
            .rev()
            .find(|e| e.term == term)
            .map(|e| e.index)
    }

    fn peer_list(&self) -> String {
        self.cfg
            .peers()
            .iter()
            .map(|p| format!("n{p}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Exposed for the integration tests, which drive nodes in-process.
pub fn describe_apply(result: &ApplyResult) -> String {
    match result {
        ApplyResult::Ok => "ok".to_string(),
        ApplyResult::Value { value } => value.clone().unwrap_or_else(|| "<none>".into()),
    }
}
