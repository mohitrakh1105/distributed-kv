//! The supervisor's little side-protocol, spoken on the port base (`6100` by
//! default — the nodes are the three ports above it).
//!
//! Two jobs. It lets a second terminal say "kill node 3" to the process that
//! owns these children — and it is the deadman line that stops nodes being
//! orphaned when the supervisor dies. On Windows there is no process group to
//! kill, so this costs one socket per node and no FFI.
//!
//! The deadman is a **positive** signal: the supervisor sends a ping every
//! second and a node that goes 4 seconds without one exits. Waiting for the
//! socket to close instead — the obvious design — does not work on Windows:
//! sockets are inheritable handles, so every node process inherits a duplicate
//! of the supervisor's end. Kill the supervisor and the connection stays open,
//! held alive by the very children that were supposed to notice it died. A
//! missing heartbeat has no such loophole, and it also catches a supervisor that
//! is hung rather than dead.

use crate::types::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Control {
    /// Sent by a node once. The supervisor then pings this connection forever;
    /// silence is what tells the node its parent is gone.
    Hello {
        node: NodeId,
    },
    /// The supervisor's liveness ping on a deadman connection.
    Ping,
    Kill {
        node: NodeId,
    },
    Start {
        node: NodeId,
    },
    Down,
    Running,
    Ok {
        message: String,
    },
    Error {
        message: String,
    },
    Nodes {
        running: Vec<NodeId>,
    },
}

impl Control {
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("control messages are serialisable")
    }

    pub fn decode(line: &str) -> Result<Control, String> {
        serde_json::from_str(line).map_err(|e| format!("{e}"))
    }
}
