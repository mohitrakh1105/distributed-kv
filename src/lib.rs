//! A three-node replicated key-value store built to be broken.
//!
//! Read `LEARNING.md` first — it explains the architecture and the experiments.
//! The short version of the layering:
//!
//! ```text
//!   dkv (CLI)  ──TCP──►  dkv-node ×3
//!                          │
//!                          ├── raft::node     the consensus loop (Figure 2)
//!                          ├── raft::log_store durable, checksummed log
//!                          ├── raft::snapshot  compaction + InstallSnapshot
//!                          ├── statemachine    the map the log produces
//!                          ├── transport       TCP, retries, per-peer tasks
//!                          └── faults          fake partitions, loss, latency
//! ```

pub mod cli;
pub mod config;
pub mod control;
pub mod faults;
pub mod logfmt;
pub mod merkle;
pub mod proto;
pub mod raft;
pub mod statemachine;
pub mod transport;
pub mod types;
