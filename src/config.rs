//! Cluster layout and the timing knobs. Defaults are deliberately slow enough
//! that a human can watch an election happen.

use crate::types::NodeId;
use std::path::PathBuf;
use std::str::FromStr;

/// Supervisor control port. Node N listens on `PORT_BASE + N`, so the default
/// cluster is 6101/6102/6103 with the supervisor on 6100. (7000-7003 would have
/// been the obvious choice; Docker Desktop on this machine already forwards
/// 7001-7003, which is exactly the kind of collision a port-base flag is for.)
pub const DEFAULT_PORT_BASE: u16 = 6100;

pub const CLUSTER: [NodeId; 3] = [1, 2, 3];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncMode {
    /// fsync every append before replying. Survives a hard kill; slowest.
    Always,
    /// fsync at most every few milliseconds. Loses only the very tail.
    Batch,
    /// Never fsync explicitly. Fast, and a power cut or TerminateProcess can
    /// lose anything the OS had not flushed.
    Never,
}

impl FromStr for FsyncMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "always" | "on" => Ok(FsyncMode::Always),
            "batch" => Ok(FsyncMode::Batch),
            "never" | "off" => Ok(FsyncMode::Never),
            other => Err(format!("unknown fsync mode '{other}' (always|batch|never)")),
        }
    }
}

impl FsyncMode {
    pub fn label(&self) -> &'static str {
        match self {
            FsyncMode::Always => "always",
            FsyncMode::Batch => "batch",
            FsyncMode::Never => "never",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// The story: client ops, commits, role changes, failures, recoveries.
    Normal,
    /// Adds every AppendEntries and index movement.
    Verbose,
    /// Adds raw frames and disk timings.
    Trace,
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "normal" => Ok(LogLevel::Normal),
            "verbose" => Ok(LogLevel::Verbose),
            "trace" => Ok(LogLevel::Trace),
            other => Err(format!(
                "unknown log level '{other}' (normal|verbose|trace)"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    pub port_base: u16,
    pub data_dir: PathBuf,

    /// Lower bound of the randomized election timeout. Randomization is what
    /// stops all three nodes campaigning in lockstep forever.
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    /// Must stay well under `election_min_ms` or followers time out while the
    /// leader is perfectly healthy and the cluster flaps.
    pub heartbeat_ms: u64,

    pub fsync: FsyncMode,
    /// Compact once the log holds more than this many entries.
    pub snapshot_threshold: u64,
    pub log_level: LogLevel,
    pub color: bool,
    /// Supervisor control port to hold a deadman connection to, if started by one.
    pub supervisor_port: Option<u16>,
}

impl Config {
    pub fn new(id: NodeId, data_dir: PathBuf) -> Self {
        Config {
            id,
            port_base: DEFAULT_PORT_BASE,
            data_dir,
            election_min_ms: 1000,
            election_max_ms: 2000,
            heartbeat_ms: 300,
            fsync: FsyncMode::Always,
            snapshot_threshold: 64,
            log_level: LogLevel::Normal,
            color: true,
            supervisor_port: None,
        }
    }

    pub fn port(&self, id: NodeId) -> u16 {
        self.port_base + id as u16
    }

    pub fn addr(&self, id: NodeId) -> String {
        format!("127.0.0.1:{}", self.port(id))
    }

    pub fn my_addr(&self) -> String {
        self.addr(self.id)
    }

    pub fn peers(&self) -> Vec<NodeId> {
        CLUSTER.iter().copied().filter(|n| *n != self.id).collect()
    }

    /// 2 of 3. Every safety property in Raft rests on two majorities always
    /// sharing at least one node.
    pub fn majority(&self) -> usize {
        CLUSTER.len() / 2 + 1
    }

    pub fn node_dir(&self) -> PathBuf {
        self.data_dir.join(format!("node-{}", self.id))
    }

    pub fn log_path(&self) -> PathBuf {
        self.node_dir().join("raft.log")
    }

    pub fn state_path(&self) -> PathBuf {
        self.node_dir().join("raft.state")
    }

    pub fn snapshot_path(&self) -> PathBuf {
        self.node_dir().join("snapshot.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_three_node_cluster_needs_two_votes_to_decide_anything() {
        let cfg = Config::new(1, PathBuf::from(".dkv"));
        assert_eq!(cfg.majority(), 2);
    }

    #[test]
    fn a_node_does_not_list_itself_as_a_peer() {
        let cfg = Config::new(2, PathBuf::from(".dkv"));
        assert_eq!(cfg.peers(), vec![1, 3]);
    }

    #[test]
    fn heartbeat_interval_stays_well_under_the_election_timeout() {
        let cfg = Config::new(1, PathBuf::from(".dkv"));
        assert!(
            cfg.heartbeat_ms * 3 <= cfg.election_min_ms,
            "heartbeats must fit several times into the shortest election timeout, \
             or a healthy leader gets voted out"
        );
    }
}
