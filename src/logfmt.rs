//! Terminal output. The whole point of this project is watching three processes
//! argue, so the log format is a feature, not plumbing.
//!
//! Format: `HH:MM:SS.mmm [nN] CAT message`, timestamps in UTC (std has no local
//! clock and a dependency for one is not worth it — all three nodes agree, which
//! is what matters when reading causality).
//!
//! Colour carries one dimension only: the node tag is coloured per node, and the
//! four-letter category owns the semantic colour. Two colour dimensions on one
//! line is a thing the eye has to decode instead of read.

use crate::config::LogLevel;
use crate::types::NodeId;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cat {
    /// A client asked for something, or got an answer.
    Clnt,
    /// Log replication: appends, acks, commits, truncations.
    Repl,
    /// Elections: timeouts, votes, campaigns.
    Elec,
    /// Role transitions. Printed as a full-width rule — these are the moments
    /// you are usually looking for when you scroll back.
    Role,
    /// Something broke: a peer went unreachable, a process died, a write failed.
    Fail,
    /// Something healed: a node came back, a follower caught up.
    Rcvr,
    /// Heartbeats. Verbose only — at 300ms these would drown everything else.
    Beat,
    /// Disk: fsync, replay, truncation of torn tails.
    Disk,
    /// Snapshots and compaction.
    Snap,
    /// Admin commands: fault injection, inspection.
    Admn,
}

impl Cat {
    pub fn tag(&self) -> &'static str {
        match self {
            Cat::Clnt => "CLNT",
            Cat::Repl => "REPL",
            Cat::Elec => "ELEC",
            Cat::Role => "ROLE",
            Cat::Fail => "FAIL",
            Cat::Rcvr => "RCVR",
            Cat::Beat => "BEAT",
            Cat::Disk => "DISK",
            Cat::Snap => "SNAP",
            Cat::Admn => "ADMN",
        }
    }

    pub fn color(&self) -> &'static str {
        match self {
            Cat::Clnt => "\x1b[1;37m",
            Cat::Repl => "\x1b[36m",
            Cat::Elec => "\x1b[33m",
            Cat::Role => "\x1b[1;35m",
            Cat::Fail => "\x1b[31m",
            Cat::Rcvr => "\x1b[32m",
            Cat::Beat => "\x1b[2;37m",
            Cat::Disk => "\x1b[2;37m",
            Cat::Snap => "\x1b[34m",
            Cat::Admn => "\x1b[33m",
        }
    }
}

pub const RESET: &str = "\x1b[0m";

/// Node tag colours. Chosen to stay legible on both light and dark terminals,
/// which rules out plain yellow and bright white.
pub fn node_color(id: NodeId) -> &'static str {
    match id {
        1 => "\x1b[36m",
        2 => "\x1b[35m",
        _ => "\x1b[34m",
    }
}

/// `true` unless the user or the environment asked for plain text.
/// Honours <https://no-color.org>.
pub fn color_enabled(flag_allows: bool) -> bool {
    flag_allows && std::env::var_os("NO_COLOR").is_none()
}

/// One log line in the node format, built outside a node. The supervisor uses
/// it so "process exited" lands in the same column layout as everything else.
pub fn format_line(id: NodeId, cat: Cat, msg: &str, color: bool) -> String {
    let ts = timestamp();
    if color {
        format!(
            "\x1b[2;37m{ts}{RESET} {}[n{id}]{RESET} {}{}{RESET} {msg}",
            node_color(id),
            cat.color(),
            cat.tag()
        )
    } else {
        format!("{ts} [n{id}] {} {msg}", cat.tag())
    }
}

/// UTC time of day with millisecond precision. Milliseconds matter here —
/// election timeouts and commit latencies both live in that range.
pub fn timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let total = now.as_secs();
    let ms = now.subsec_millis();
    let (h, m, s) = ((total / 3600) % 24, (total / 60) % 60, total % 60);
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

pub struct Logger {
    id: NodeId,
    level: LogLevel,
    color: bool,
    /// A plain-text copy per node, so a scrolled-away election can still be
    /// read after the fact. Behind a mutex because every task logs.
    file: Option<Mutex<File>>,
}

impl Logger {
    pub fn new(id: NodeId, level: LogLevel, color: bool) -> Self {
        Logger {
            id,
            level,
            color: color_enabled(color),
            file: None,
        }
    }

    pub fn with_file(mut self, path: &Path) -> Self {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match File::options().create(true).append(true).open(path) {
            Ok(f) => self.file = Some(Mutex::new(f)),
            Err(e) => eprintln!("cannot open log file {}: {e}", path.display()),
        }
        self
    }

    pub fn level(&self) -> LogLevel {
        self.level
    }

    /// Part of the story. Always printed.
    pub fn say(&self, cat: Cat, msg: impl AsRef<str>) {
        self.emit(cat, LogLevel::Normal, msg.as_ref());
    }

    /// Mechanism rather than story — every AppendEntries, every index move.
    pub fn detail(&self, cat: Cat, msg: impl AsRef<str>) {
        self.emit(cat, LogLevel::Verbose, msg.as_ref());
    }

    /// Raw frames, byte counts, fsync timings.
    pub fn trace(&self, cat: Cat, msg: impl AsRef<str>) {
        self.emit(cat, LogLevel::Trace, msg.as_ref());
    }

    /// Role changes get a rule line because they are the events you scroll back
    /// looking for.
    pub fn role_change(&self, from: &str, to: &str, term: u64, note: &str) {
        let body = if note.is_empty() {
            format!(" n{}  {from} -> {to}  term {term} ", self.id)
        } else {
            format!(" n{}  {from} -> {to}  term {term} ({note}) ", self.id)
        };
        let width: usize = 74;
        let pad = width.saturating_sub(body.len());
        let left = pad / 2;
        let line = format!("{}{}{}", "-".repeat(left), body, "-".repeat(pad - left));
        if self.color {
            println!("{}{}{}", Cat::Role.color(), line, RESET);
        } else {
            println!("{line}");
        }
        self.write_file(&line);
    }

    fn emit(&self, cat: Cat, needed: LogLevel, msg: &str) {
        if self.level < needed {
            return;
        }
        let ts = timestamp();
        let plain = format!("{ts} [n{}] {} {msg}", self.id, cat.tag());
        if self.color {
            println!(
                "\x1b[2;37m{ts}{RESET} {}[n{}]{RESET} {}{}{RESET} {msg}",
                node_color(self.id),
                self.id,
                cat.color(),
                cat.tag()
            );
        } else {
            println!("{plain}");
        }
        self.write_file(&plain);
    }

    fn write_file(&self, line: &str) {
        if let Some(file) = &self.file
            && let Ok(mut f) = file.lock()
        {
            let _ = writeln!(f, "{line}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timestamp_is_a_fixed_width_time_of_day() {
        let ts = timestamp();
        assert_eq!(ts.len(), 12, "HH:MM:SS.mmm is 12 characters: {ts}");
        assert_eq!(&ts[2..3], ":");
        assert_eq!(&ts[8..9], ".");
    }

    #[test]
    fn every_category_tag_is_four_characters_so_columns_line_up() {
        for cat in [
            Cat::Clnt,
            Cat::Repl,
            Cat::Elec,
            Cat::Role,
            Cat::Fail,
            Cat::Rcvr,
            Cat::Beat,
            Cat::Disk,
            Cat::Snap,
            Cat::Admn,
        ] {
            assert_eq!(cat.tag().len(), 4, "{:?} breaks the column", cat);
        }
    }

    #[test]
    fn verbose_output_is_suppressed_at_normal_level() {
        let logger = Logger::new(1, LogLevel::Normal, false);
        assert!(logger.level() < LogLevel::Verbose);
    }
}
