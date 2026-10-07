//! The thing the log is replicating: a map, plus the small table that makes
//! retries safe.
//!
//! Nothing here knows about Raft. It takes committed entries in index order and
//! folds them into state — which is exactly why every node lands on the same
//! answer given the same log. Determinism is the contract: no clocks, no
//! randomness, no iteration over a `HashMap`.

use crate::merkle;
use crate::proto::{ClientRecord, DedupEntry, SnapshotPayload};
use crate::types::{ApplyResult, Command, LogEntry, LogIndex};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct StateMachine {
    kv: BTreeMap<String, String>,
    /// `client_id -> (highest seq applied, the answer we gave)`.
    ///
    /// This lives inside the state machine rather than at request admission
    /// because two leaders in different terms can both *admit* the same request;
    /// only apply order decides which one actually happened. It is also why the
    /// table has to travel inside the snapshot.
    clients: BTreeMap<u64, DedupEntry>,
    pub last_applied: LogIndex,
}

pub struct Applied {
    pub result: ApplyResult,
    /// The request had already been applied; the cached answer was returned and
    /// the map was left alone.
    pub duplicate: bool,
}

impl StateMachine {
    pub fn new() -> Self {
        StateMachine::default()
    }

    /// Fold one committed entry in. Callers must pass entries in index order and
    /// only after commit — applying early makes uncommitted writes briefly
    /// visible, and they can still be truncated afterwards.
    pub fn apply(&mut self, entry: &LogEntry) -> Applied {
        // `last_applied` advances even for a duplicate: the entry occupies an
        // index whether or not it changed anything.
        self.last_applied = entry.index;

        if !entry.is_internal()
            && let Some(seen) = self.clients.get(&entry.client_id)
            && seen.seq >= entry.seq
        {
            return Applied {
                result: ApplyResult::Value {
                    value: seen.value.clone(),
                },
                duplicate: true,
            };
        }

        let result = match &entry.command {
            Command::Set { key, value } => {
                self.kv.insert(key.clone(), value.clone());
                ApplyResult::Ok
            }
            Command::Delete { key } => {
                self.kv.remove(key);
                ApplyResult::Ok
            }
            Command::Noop => ApplyResult::Ok,
        };

        if !entry.is_internal() {
            self.clients.insert(
                entry.client_id,
                DedupEntry {
                    seq: entry.seq,
                    value: None,
                },
            );
        }

        Applied {
            result,
            duplicate: false,
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.kv.get(key).cloned()
    }

    pub fn keys(&self) -> usize {
        self.kv.len()
    }

    pub fn clients(&self) -> usize {
        self.clients.len()
    }

    pub fn kv(&self) -> &BTreeMap<String, String> {
        &self.kv
    }

    pub fn state_hash(&self) -> u32 {
        merkle::root(&self.kv)
    }

    pub fn snapshot(&self) -> SnapshotPayload {
        SnapshotPayload {
            kv: self.kv.clone(),
            clients: self
                .clients
                .iter()
                .map(|(client_id, entry)| ClientRecord {
                    client_id: *client_id,
                    seq: entry.seq,
                    value: entry.value.clone(),
                })
                .collect(),
        }
    }

    /// Replace everything. Used on startup from a local snapshot file, and when
    /// a leader ships one to a follower that has fallen too far behind.
    pub fn restore(&mut self, payload: SnapshotPayload, last_included_index: LogIndex) {
        self.kv = payload.kv;
        self.clients = payload
            .clients
            .into_iter()
            .map(|r| {
                (
                    r.client_id,
                    DedupEntry {
                        seq: r.seq,
                        value: r.value,
                    },
                )
            })
            .collect();
        self.last_applied = last_included_index;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: LogIndex, client_id: u64, seq: u64, command: Command) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            client_id,
            seq,
            command,
        }
    }

    fn set(index: LogIndex, client_id: u64, seq: u64, key: &str, value: &str) -> LogEntry {
        entry(
            index,
            client_id,
            seq,
            Command::Set {
                key: key.into(),
                value: value.into(),
            },
        )
    }

    #[test]
    fn a_set_becomes_readable_and_a_delete_removes_it() {
        let mut sm = StateMachine::new();
        sm.apply(&set(1, 7, 1, "name", "mohit"));
        assert_eq!(sm.get("name"), Some("mohit".to_string()));
        sm.apply(&entry(2, 7, 2, Command::Delete { key: "name".into() }));
        assert_eq!(sm.get("name"), None);
        assert_eq!(sm.last_applied, 2);
    }

    #[test]
    fn a_retried_request_is_applied_once_even_though_it_arrives_twice() {
        let mut sm = StateMachine::new();
        sm.apply(&set(1, 7, 1, "count", "first"));
        sm.apply(&set(2, 7, 2, "count", "second"));
        // The client retried seq 1; it landed at a fresh log index, but the
        // dedup table has already seen a higher seq from this client.
        let replay = sm.apply(&set(3, 7, 1, "count", "first"));
        assert!(replay.duplicate, "an old seq must not be applied again");
        assert_eq!(
            sm.get("count"),
            Some("second".to_string()),
            "the replay must not roll the value back"
        );
        assert_eq!(sm.last_applied, 3, "the index is consumed regardless");
    }

    #[test]
    fn two_different_clients_do_not_share_a_sequence_space() {
        let mut sm = StateMachine::new();
        sm.apply(&set(1, 7, 1, "a", "from-seven"));
        let other = sm.apply(&set(2, 8, 1, "b", "from-eight"));
        assert!(!other.duplicate, "client 8 has its own seq 1");
        assert_eq!(sm.get("b"), Some("from-eight".to_string()));
    }

    #[test]
    fn an_internal_noop_changes_nothing_but_still_consumes_its_index() {
        let mut sm = StateMachine::new();
        sm.apply(&set(1, 7, 1, "name", "mohit"));
        sm.apply(&entry(2, 0, 0, Command::Noop));
        assert_eq!(sm.keys(), 1);
        assert_eq!(sm.clients(), 1, "a noop has no client to remember");
        assert_eq!(sm.last_applied, 2);
    }

    #[test]
    fn a_restored_snapshot_still_remembers_which_requests_were_applied() {
        let mut original = StateMachine::new();
        original.apply(&set(1, 7, 1, "name", "mohit"));
        original.apply(&set(2, 7, 2, "city", "pune"));

        let mut restored = StateMachine::new();
        restored.restore(original.snapshot(), original.last_applied);

        assert_eq!(restored.state_hash(), original.state_hash());
        let replay = restored.apply(&set(3, 7, 2, "city", "mumbai"));
        assert!(
            replay.duplicate,
            "without the dedup table in the snapshot this retry would be applied twice"
        );
        assert_eq!(restored.get("city"), Some("pune".to_string()));
    }

    #[test]
    fn the_same_entries_in_the_same_order_produce_the_same_hash_on_every_node() {
        let entries = vec![
            set(1, 7, 1, "name", "mohit"),
            set(2, 7, 2, "city", "pune"),
            entry(3, 7, 3, Command::Delete { key: "name".into() }),
            set(4, 8, 1, "lang", "rust"),
        ];
        let mut a = StateMachine::new();
        let mut b = StateMachine::new();
        for e in &entries {
            a.apply(e);
        }
        for e in &entries {
            b.apply(e);
        }
        assert_eq!(a.state_hash(), b.state_hash());
        assert_eq!(a.last_applied, b.last_applied);
    }
}
