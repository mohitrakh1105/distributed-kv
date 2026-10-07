//! Everything the `dkv` binary does that is not consensus: talking to nodes,
//! babysitting their processes, and the demos, dashboard and bench built on top.

pub mod bench;
pub mod commands;
pub mod demo;
pub mod supervise;
pub mod watch;
