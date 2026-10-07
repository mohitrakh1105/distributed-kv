//! Snapshot file handling.
//!
//! A snapshot is the state machine plus the index it is current as of. Once one
//! exists, every log entry up to that index is redundant and gets compacted
//! away — which is the only thing stopping the log from growing forever.
//!
//! Stored as JSON rather than a packed binary format, on the same principle as
//! the wire protocol: being able to open the file and read it is worth more here
//! than the bytes it costs.

use crate::proto::SnapshotPayload;
use crate::types::{LogIndex, StoreError, Term};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub payload: SnapshotPayload,
}

pub fn load(path: &Path) -> Result<Option<StoredSnapshot>, StoreError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let snap: StoredSnapshot =
                serde_json::from_slice(&bytes).map_err(|e| StoreError::Decode(format!("{e}")))?;
            Ok(Some(snap))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StoreError::Io(e)),
    }
}

/// Writes the snapshot durably and returns its size. This must complete *before*
/// the log is compacted — the ordering is what makes a crash mid-compaction
/// recoverable rather than fatal.
pub fn save(path: &Path, snapshot: &StoredSnapshot) -> Result<u64, StoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(snapshot).expect("a snapshot is serialisable");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(bytes.len() as u64)
}

pub fn size_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ClientRecord;
    use std::collections::BTreeMap;

    #[test]
    fn a_saved_snapshot_round_trips_with_its_dedup_table() {
        let dir = std::env::temp_dir().join(format!("dkv-snap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("snapshot.json");

        let mut kv = BTreeMap::new();
        kv.insert("name".to_string(), "mohit".to_string());
        let clients = vec![ClientRecord {
            client_id: 9,
            seq: 4,
            value: None,
        }];

        let snap = StoredSnapshot {
            last_included_index: 12,
            last_included_term: 3,
            payload: SnapshotPayload { kv, clients },
        };
        let bytes = save(&path, &snap).expect("save");
        assert!(bytes > 0);

        let back = load(&path).expect("load").expect("present");
        assert_eq!(back.last_included_index, 12);
        assert_eq!(back.last_included_term, 3);
        assert_eq!(back.payload.kv["name"], "mohit");
        assert_eq!(back.payload.clients[0].client_id, 9);
        assert_eq!(back.payload.clients[0].seq, 4);
    }

    #[test]
    fn a_node_with_no_snapshot_yet_loads_nothing_rather_than_failing() {
        let path = std::env::temp_dir().join("dkv-no-such-snapshot.json");
        let _ = std::fs::remove_file(&path);
        assert!(load(&path).expect("missing is fine").is_none());
    }
}
