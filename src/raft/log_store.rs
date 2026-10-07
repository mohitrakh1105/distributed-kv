//! The durable Raft log.
//!
//! Record layout on disk:
//!
//! ```text
//! [u32 le payload_len][u32 le crc32(payload)][payload: JSON LogEntry]['\n']
//! ```
//!
//! The trailing newline is not counted in `payload_len`. It costs one byte per
//! entry and makes the file greppable, which matters when the point of the
//! exercise is looking at what got written.
//!
//! A hard kill mid-append leaves a half-written record. Replay detects that
//! (short read, or a checksum that does not match) and truncates the tail —
//! which is correct, because an entry that was never fully written was also
//! never acknowledged to anybody. The one case that is *not* recoverable is
//! damage reaching below the snapshot: those entries are already assumed
//! durable, so the node refuses to start rather than come up quietly missing
//! committed state.

use crate::config::FsyncMode;
use crate::types::{LogEntry, LogIndex, StoreError, Term};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const HEADER_BYTES: usize = 8;
/// How long `FsyncMode::Batch` is willing to leave writes unflushed.
const BATCH_FSYNC_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug, Default, PartialEq)]
pub struct ReplayReport {
    pub entries_loaded: usize,
    /// Byte offset where a damaged tail started, if there was one.
    pub truncated_at: Option<u64>,
    pub bytes_dropped: usize,
    /// Entries the snapshot already covers, found still sitting in the log.
    /// Means a previous compaction was interrupted between making the snapshot
    /// durable and rewriting the log — recoverable, but worth reporting.
    pub pre_snapshot_skipped: usize,
}

pub struct LogStore {
    path: PathBuf,
    /// `None` only while the file is being rewritten: Windows refuses to rename
    /// over a path that still has an open handle, so the handle is dropped first.
    file: Option<File>,
    /// Entries *after* the snapshot. Index `i` here is log index
    /// `last_included_index + 1 + i` — see `offset_for`, which is the only place
    /// that arithmetic is allowed to happen.
    entries: Vec<LogEntry>,
    last_included_index: LogIndex,
    last_included_term: Term,
    fsync: FsyncMode,
    last_fsync: Instant,
}

impl LogStore {
    /// Opens (or creates) the log and replays it. `last_included_*` come from the
    /// snapshot file, and are what the log is expected to continue from.
    pub fn open(
        path: &Path,
        fsync: FsyncMode,
        last_included_index: LogIndex,
        last_included_term: Term,
    ) -> Result<(LogStore, ReplayReport), StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let (entries, report) = replay(path, last_included_index)?;
        let file = File::options().create(true).append(true).open(path)?;
        let mut store = LogStore {
            path: path.to_path_buf(),
            file: Some(file),
            entries,
            last_included_index,
            last_included_term,
            fsync,
            last_fsync: Instant::now(),
        };
        // Finish the compaction that was interrupted, so the next restart has
        // nothing left to skip.
        if report.pre_snapshot_skipped > 0 {
            store.rewrite()?;
        }
        Ok((store, report))
    }

    /// The single place log-index-to-slot arithmetic lives. Inlining this at
    /// call sites is how off-by-ones get in, especially after compaction moves
    /// the base.
    fn offset_for(&self, index: LogIndex) -> Option<usize> {
        if index <= self.last_included_index || index > self.last_index() {
            return None;
        }
        Some((index - self.last_included_index - 1) as usize)
    }

    pub fn first_index(&self) -> LogIndex {
        self.last_included_index + 1
    }

    pub fn last_index(&self) -> LogIndex {
        self.last_included_index + self.entries.len() as LogIndex
    }

    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.last_included_term)
    }

    pub fn snapshot_index(&self) -> LogIndex {
        self.last_included_index
    }

    pub fn snapshot_term(&self) -> Term {
        self.last_included_term
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        self.offset_for(index).map(|off| &self.entries[off])
    }

    /// The term at `index`, including the two sentinels: index 0 is term 0, and
    /// the snapshot boundary carries its own term. `None` means "compacted away
    /// or not reached yet", which callers must treat as "I cannot answer that".
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.last_included_index {
            return Some(self.last_included_term);
        }
        self.entry(index).map(|e| e.term)
    }

    pub fn entries_from(&self, index: LogIndex, max: usize) -> Vec<LogEntry> {
        match self.offset_for(index) {
            Some(off) => {
                let end = (off + max).min(self.entries.len());
                self.entries[off..end].to_vec()
            }
            None => Vec::new(),
        }
    }

    pub fn all_entries(&self) -> &[LogEntry] {
        &self.entries
    }

    /// Appends and persists. Returns once the bytes are at least in the OS page
    /// cache, and — under `FsyncMode::Always` — on the platter. The caller may
    /// only reply to a client *after* this returns.
    pub fn append(&mut self, new: &[LogEntry]) -> Result<(), StoreError> {
        if new.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for entry in new {
            let expected = self.last_index() + 1 + (buf_entry_count(&buf) as LogIndex);
            if entry.index != expected {
                return Err(StoreError::IndexGap {
                    expected,
                    found: entry.index,
                });
            }
            encode_record(entry, &mut buf);
        }
        let file = self.file.as_mut().expect("log file is open");
        file.write_all(&buf)?;
        self.entries.extend_from_slice(new);
        self.maybe_fsync()?;
        Ok(())
    }

    fn maybe_fsync(&mut self) -> Result<(), StoreError> {
        let file = self.file.as_mut().expect("log file is open");
        match self.fsync {
            FsyncMode::Always => {
                file.sync_data()?;
                self.last_fsync = Instant::now();
            }
            FsyncMode::Batch => {
                if self.last_fsync.elapsed() >= BATCH_FSYNC_INTERVAL {
                    file.sync_data()?;
                    self.last_fsync = Instant::now();
                }
            }
            FsyncMode::Never => {}
        }
        Ok(())
    }

    /// Drops `index` and everything after it. Called when a follower finds its
    /// log disagreeing with the leader's — and *only* at a proven mismatch, never
    /// speculatively, or a delayed duplicate AppendEntries deletes good entries.
    pub fn truncate_from(&mut self, index: LogIndex) -> Result<usize, StoreError> {
        let Some(off) = self.offset_for(index) else {
            return Ok(0);
        };
        let dropped = self.entries.len() - off;
        self.entries.truncate(off);
        self.rewrite()?;
        Ok(dropped)
    }

    /// Discards everything up to and including `index`, which the snapshot now
    /// covers. The snapshot must already be durable before this is called.
    pub fn compact(&mut self, index: LogIndex, term: Term) -> Result<usize, StoreError> {
        if index <= self.last_included_index {
            return Ok(0);
        }
        let keep_from = self.offset_for(index).map(|off| off + 1);
        let dropped = match keep_from {
            Some(off) => {
                self.entries.drain(..off);
                off
            }
            None => {
                // The snapshot covers past the end of the log, which happens on a
                // follower that just installed one from the leader.
                let dropped = self.entries.len();
                self.entries.clear();
                dropped
            }
        };
        self.last_included_index = index;
        self.last_included_term = term;
        self.rewrite()?;
        Ok(dropped)
    }

    /// Rewrites the whole file from memory, atomically. Cheap because the log is
    /// kept short by compaction, and it avoids the partial-file states that
    /// in-place truncation can leave behind.
    fn rewrite(&mut self) -> Result<(), StoreError> {
        let tmp = self.path.with_extension("log.tmp");
        let mut buf = Vec::new();
        for entry in &self.entries {
            encode_record(entry, &mut buf);
        }
        {
            let mut out = File::create(&tmp)?;
            out.write_all(&buf)?;
            out.sync_all()?;
        }
        // Windows will not rename over a path that still has an open handle.
        self.file = None;
        std::fs::rename(&tmp, &self.path)?;
        self.file = Some(File::options().append(true).open(&self.path)?);
        Ok(())
    }
}

fn buf_entry_count(buf: &[u8]) -> usize {
    // Records are self-delimiting; counting them back out of the buffer keeps
    // `append` able to validate index continuity across a multi-entry batch.
    let mut count = 0;
    let mut pos = 0;
    while pos + HEADER_BYTES <= buf.len() {
        let len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += HEADER_BYTES + len + 1;
        count += 1;
    }
    count
}

fn encode_record(entry: &LogEntry, out: &mut Vec<u8>) {
    let payload = serde_json::to_vec(entry).expect("a LogEntry is serialisable");
    let crc = crc32fast::hash(&payload);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&payload);
    out.push(b'\n');
}

/// Reads the file back, stopping at the first damaged record and truncating
/// there. Anything after damage is unreachable anyway: Raft's log is a sequence,
/// so a hole in the middle is the same as an end.
fn replay(
    path: &Path,
    last_included_index: LogIndex,
) -> Result<(Vec<LogEntry>, ReplayReport), StoreError> {
    if !path.exists() {
        return Ok((Vec::new(), ReplayReport::default()));
    }
    let mut reader = BufReader::new(File::open(path)?);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;

    let mut entries: Vec<LogEntry> = Vec::new();
    let mut pos: usize = 0;
    let mut damaged_at: Option<u64> = None;
    let mut skipped = 0usize;

    while pos < bytes.len() {
        let start = pos;
        if pos + HEADER_BYTES > bytes.len() {
            damaged_at = Some(start as u64);
            break;
        }
        let len = u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
            as usize;
        let crc = u32::from_le_bytes([
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]);
        pos += HEADER_BYTES;
        // `+ 1` for the trailing newline, whose absence is itself a torn write.
        if pos + len + 1 > bytes.len() {
            damaged_at = Some(start as u64);
            break;
        }
        let payload = &bytes[pos..pos + len];
        if crc32fast::hash(payload) != crc || bytes[pos + len] != b'\n' {
            damaged_at = Some(start as u64);
            break;
        }
        pos += len + 1;

        let entry: LogEntry = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(_) => {
                damaged_at = Some(start as u64);
                break;
            }
        };
        if entries.is_empty() && entry.index <= last_included_index {
            // Compaction writes the snapshot first and rewrites the log second.
            // Crash in between and these leftovers are still here; the snapshot
            // already contains them, so skip rather than panic.
            skipped += 1;
            continue;
        }
        let expected = if let Some(prev) = entries.last() {
            prev.index + 1
        } else {
            last_included_index + 1
        };
        if entry.index != expected {
            if entries.is_empty() {
                // A hole between the snapshot and the log: nothing can bridge it,
                // and coming up would mean silently serving a state machine with
                // missing history.
                return Err(StoreError::LogDoesNotFollowSnapshot {
                    first_index: entry.index,
                    snapshot_at: last_included_index,
                });
            }
            // Mid-file disagreement: treat it as a damaged tail.
            damaged_at = Some(start as u64);
            break;
        }
        entries.push(entry);
    }

    let mut report = ReplayReport {
        entries_loaded: entries.len(),
        truncated_at: damaged_at,
        bytes_dropped: 0,
        pre_snapshot_skipped: skipped,
    };

    if let Some(offset) = damaged_at {
        report.bytes_dropped = bytes.len().saturating_sub(offset as usize);
        // Only safe because the surviving prefix still continues the snapshot;
        // an entry that was never completely written was never acknowledged
        // either, so nothing that was promised to a client is being dropped.
        let file = File::options().write(true).open(path)?;
        file.set_len(offset)?;
        file.sync_all()?;
    }

    Ok((entries, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;
    use std::io::Seek;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("dkv-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn entry(index: LogIndex, term: Term, key: &str) -> LogEntry {
        LogEntry {
            index,
            term,
            client_id: 1,
            seq: index,
            command: Command::Set {
                key: key.into(),
                value: format!("v{index}"),
            },
        }
    }

    #[test]
    fn entries_survive_a_close_and_reopen() {
        let dir = temp_dir("reopen");
        let path = dir.join("raft.log");
        {
            let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
            log.append(&[entry(1, 1, "a"), entry(2, 1, "b")])
                .expect("append");
            assert_eq!(log.last_index(), 2);
        }
        let (log, report) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("reopen");
        assert_eq!(report.entries_loaded, 2);
        assert_eq!(report.truncated_at, None);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.entry(2).expect("entry 2").term, 1);
    }

    #[test]
    fn a_half_written_record_from_a_hard_kill_is_truncated_not_fatal() {
        let dir = temp_dir("torn");
        let path = dir.join("raft.log");
        {
            let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
            log.append(&[entry(1, 1, "a"), entry(2, 1, "b")])
                .expect("append");
        }
        // Simulate the process dying midway through writing entry 3.
        let mut f = File::options()
            .append(true)
            .open(&path)
            .expect("append handle");
        f.write_all(&[64, 0, 0, 0, 9, 9, 9, 9, b'{', b'"'])
            .expect("partial write");
        drop(f);

        let (log, report) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("reopen");
        assert_eq!(log.last_index(), 2, "the two complete entries survive");
        assert!(report.truncated_at.is_some(), "the torn tail was detected");
        assert!(report.bytes_dropped > 0);

        // And the file is now clean enough to append to again.
        let (mut log, report) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("reopen 2");
        assert_eq!(report.truncated_at, None, "truncation was persisted");
        log.append(&[entry(3, 2, "c")])
            .expect("append after repair");
        assert_eq!(log.last_index(), 3);
    }

    #[test]
    fn a_flipped_byte_in_a_payload_is_caught_by_the_checksum() {
        let dir = temp_dir("crc");
        let path = dir.join("raft.log");
        {
            let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
            log.append(&[entry(1, 1, "a"), entry(2, 1, "b")])
                .expect("append");
        }
        let mut f = File::options()
            .read(true)
            .write(true)
            .open(&path)
            .expect("rw");
        // Byte 8 is the first byte of the first payload.
        f.seek(std::io::SeekFrom::Start(20)).expect("seek");
        f.write_all(b"X").expect("corrupt");
        drop(f);

        let (log, report) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("reopen");
        assert_eq!(
            log.last_index(),
            0,
            "corruption at entry 1 drops everything after"
        );
        assert_eq!(report.truncated_at, Some(0));
    }

    #[test]
    fn a_hole_between_the_snapshot_and_the_log_refuses_to_start() {
        let dir = temp_dir("hole");
        let path = dir.join("raft.log");
        {
            // A log that legitimately continues from index 15...
            let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 14, 3).expect("open");
            log.append(&[entry(15, 3, "a")]).expect("append");
        }
        // ...paired with a snapshot that only reaches index 10. Indexes 11-14
        // exist nowhere, so this node cannot serve reads at all.
        match LogStore::open(&path, FsyncMode::Always, 10, 3) {
            Err(StoreError::LogDoesNotFollowSnapshot {
                first_index,
                snapshot_at,
            }) => assert_eq!((first_index, snapshot_at), (15, 10)),
            Err(other) => panic!("expected a refusal to start, got {other}"),
            Ok(_) => panic!("expected a refusal to start, but the node came up"),
        }
    }

    #[test]
    fn a_compaction_interrupted_before_the_log_rewrite_is_finished_on_restart() {
        let dir = temp_dir("interrupted-compaction");
        let path = dir.join("raft.log");
        {
            let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
            for i in 1..=6 {
                log.append(&[entry(i, 1, "k")]).expect("append");
            }
        }
        // The snapshot through index 4 made it to disk; the log rewrite did not.
        let (log, report) = LogStore::open(&path, FsyncMode::Always, 4, 1).expect("reopen");
        assert_eq!(report.pre_snapshot_skipped, 4);
        assert_eq!(report.entries_loaded, 2, "entries 5 and 6 are still needed");
        assert_eq!(log.first_index(), 5);
        assert_eq!(log.last_index(), 6);

        drop(log);
        let (log, report) = LogStore::open(&path, FsyncMode::Always, 4, 1).expect("reopen 2");
        assert_eq!(
            report.pre_snapshot_skipped, 0,
            "the interrupted compaction was completed on the first restart"
        );
        assert_eq!(log.last_index(), 6);
    }

    #[test]
    fn truncating_from_an_index_drops_it_and_everything_after_it() {
        let dir = temp_dir("truncate");
        let path = dir.join("raft.log");
        let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
        log.append(&[entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")])
            .expect("append");
        let dropped = log.truncate_from(2).expect("truncate");
        assert_eq!(dropped, 2);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);

        // The truncation is durable, not just in memory.
        drop(log);
        let (log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("reopen");
        assert_eq!(log.last_index(), 1);
    }

    #[test]
    fn index_arithmetic_still_works_after_compaction_moves_the_base() {
        let dir = temp_dir("compact");
        let path = dir.join("raft.log");
        let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
        for i in 1..=6 {
            log.append(&[entry(i, 1, "k")]).expect("append");
        }
        log.compact(4, 1).expect("compact");

        assert_eq!(log.first_index(), 5);
        assert_eq!(log.last_index(), 6);
        assert_eq!(log.entry_count(), 2);
        assert!(log.entry(4).is_none(), "entry 4 is inside the snapshot now");
        assert_eq!(log.entry(5).expect("entry 5").index, 5);
        assert_eq!(
            log.term_at(4),
            Some(1),
            "the snapshot boundary keeps its term"
        );
        assert_eq!(log.entries_from(5, 10).len(), 2);

        drop(log);
        let (log, report) = LogStore::open(&path, FsyncMode::Always, 4, 1).expect("reopen");
        assert_eq!(report.entries_loaded, 2);
        assert_eq!(log.last_index(), 6);
    }

    #[test]
    fn appending_out_of_order_is_rejected_rather_than_silently_written() {
        let dir = temp_dir("gap");
        let path = dir.join("raft.log");
        let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
        log.append(&[entry(1, 1, "a")]).expect("append");
        match log.append(&[entry(3, 1, "c")]) {
            Err(StoreError::IndexGap { expected, found }) => {
                assert_eq!((expected, found), (2, 3))
            }
            other => panic!("expected an index gap, got {other:?}"),
        }
    }

    #[test]
    fn a_multi_entry_batch_validates_continuity_across_the_whole_batch() {
        let dir = temp_dir("batch-gap");
        let path = dir.join("raft.log");
        let (mut log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
        match log.append(&[entry(1, 1, "a"), entry(3, 1, "c")]) {
            Err(StoreError::IndexGap { expected, found }) => {
                assert_eq!((expected, found), (2, 3))
            }
            other => panic!("expected an index gap, got {other:?}"),
        }
    }

    #[test]
    fn the_term_of_index_zero_is_zero_so_an_empty_log_compares_cleanly() {
        let dir = temp_dir("zero");
        let path = dir.join("raft.log");
        let (log, _) = LogStore::open(&path, FsyncMode::Always, 0, 0).expect("open");
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
    }
}
