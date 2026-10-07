//! `dkv up`: the process that owns the three node processes.
//!
//! It does exactly one job — lifecycle. Spawn the nodes, weave their output into
//! one stream, kill and restart them on request, and take them down with itself.
//! Every other command talks to the nodes directly, so nothing here is in the
//! path of a `get` or a `set`.
//!
//! Two Windows details shaped this file:
//!
//! - There is no process group to signal, so orphan prevention is a **deadman
//!   socket**: each node holds a connection to the control port open, and exits
//!   when it closes. Kill the supervisor however you like; the children follow
//!   within milliseconds. No Job Objects, no FFI.
//! - Ctrl-C in a console is delivered to every process attached to it. Children
//!   are spawned with `CREATE_NEW_PROCESS_GROUP` so the supervisor gets to
//!   narrate the shutdown instead of all four processes dying at once.

use crate::config::CLUSTER;
use crate::control::Control;
use crate::logfmt::{Cat, format_line};
use crate::types::NodeId;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{Notify, mpsc};

#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

/// How often the supervisor proves it is alive on each deadman connection.
/// Nodes give up after `DEADMAN_TIMEOUT`, which is several pings' worth so a
/// scheduling hiccup never kills a healthy cluster.
pub const DEADMAN_PING: Duration = Duration::from_secs(1);
pub const DEADMAN_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub port_base: u16,
    pub data_dir: PathBuf,
    pub election_min: u64,
    pub election_max: u64,
    pub heartbeat: u64,
    pub fsync: String,
    pub snapshot_threshold: u64,
    pub log: String,
    pub color: bool,
}

impl Default for NodeOptions {
    fn default() -> Self {
        NodeOptions {
            port_base: crate::config::DEFAULT_PORT_BASE,
            data_dir: PathBuf::from(".dkv"),
            election_min: 1000,
            election_max: 2000,
            heartbeat: 300,
            fsync: "always".into(),
            snapshot_threshold: 64,
            log: "normal".into(),
            color: true,
        }
    }
}

struct Shared {
    children: Mutex<HashMap<NodeId, Child>>,
    /// Nodes we killed on purpose, so their exit line says "killed" rather than
    /// looking like a crash.
    killed: Mutex<HashSet<NodeId>>,
}

pub struct Supervisor {
    opts: NodeOptions,
    shared: Arc<Shared>,
    line_tx: mpsc::Sender<String>,
    /// Every node's stdout, plus whatever the caller pushes through
    /// `line_sender` (demo narration), in arrival order. Taken by whoever is
    /// going to print, so the supervisor itself stays usable afterwards.
    lines: Option<mpsc::Receiver<String>>,
    pub shutdown: Arc<Notify>,
}

impl Supervisor {
    /// Binds the control port and starts all three nodes. Fails if another
    /// supervisor already holds the port — which is also how `dkv` discovers
    /// that a cluster is running: connect, and see.
    pub async fn start(opts: NodeOptions, fresh: bool) -> Result<Supervisor, String> {
        let control_addr = format!("127.0.0.1:{}", opts.port_base);
        let listener = TcpListener::bind(&control_addr).await.map_err(|e| {
            format!(
                "cannot bind control port {control_addr}: {e}\n  (is `dkv up` already running? use `dkv down` first)"
            )
        })?;

        if fresh {
            let _ = std::fs::remove_dir_all(&opts.data_dir);
        }
        std::fs::create_dir_all(&opts.data_dir).map_err(|e| format!("{e}"))?;

        let (line_tx, lines) = mpsc::channel::<String>(4096);
        let shared = Arc::new(Shared {
            children: Mutex::new(HashMap::new()),
            killed: Mutex::new(HashSet::new()),
        });
        let shutdown = Arc::new(Notify::new());

        let sup = Supervisor {
            opts,
            shared,
            line_tx,
            lines: Some(lines),
            shutdown,
        };
        sup.spawn_control_server(listener);
        for id in CLUSTER {
            sup.start_node(id)?;
        }
        Ok(sup)
    }

    pub fn line_sender(&self) -> mpsc::Sender<String> {
        self.line_tx.clone()
    }

    /// Hands the merged output stream to the caller. Only one printer can exist,
    /// so a second call returns `None`.
    pub fn take_lines(&mut self) -> Option<mpsc::Receiver<String>> {
        self.lines.take()
    }

    pub fn options(&self) -> &NodeOptions {
        &self.opts
    }

    pub fn start_node(&self, id: NodeId) -> Result<(), String> {
        start_node(&self.opts, id, &self.shared, &self.line_tx)
    }

    /// Hard kill — TerminateProcess, no unwinding, no flushing. Deliberately the
    /// same thing a power cut does to a node, because that is the failure the
    /// log's checksums and fsyncs exist to survive.
    pub fn kill_node(&self, id: NodeId) -> Result<(), String> {
        kill_node(id, &self.shared)
    }

    pub fn running(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self
            .shared
            .children
            .lock()
            .map(|c| c.keys().copied().collect())
            .unwrap_or_default();
        ids.sort_unstable();
        ids
    }

    pub async fn shutdown(&self) {
        let ids = self.running();
        for id in ids {
            let _ = self.kill_node(id);
        }
        // Give the exit lines a moment to drain so they are actually seen.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    fn spawn_control_server(&self, listener: TcpListener) {
        let shared = Arc::clone(&self.shared);
        let opts = self.opts.clone();
        let line_tx = self.line_tx.clone();
        let shutdown = Arc::clone(&self.shutdown);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let shared = Arc::clone(&shared);
                let opts = opts.clone();
                let line_tx = line_tx.clone();
                let shutdown = Arc::clone(&shutdown);
                tokio::spawn(serve_control(stream, shared, opts, line_tx, shutdown));
            }
        });
    }
}

async fn serve_control(
    stream: TcpStream,
    shared: Arc<Shared>,
    opts: NodeOptions,
    line_tx: mpsc::Sender<String>,
    shutdown: Arc<Notify>,
) {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();
    let Ok(Some(first)) = lines.next_line().await else {
        return;
    };
    let cmd = match Control::decode(&first) {
        Ok(c) => c,
        Err(e) => {
            let _ = write_half
                .write_all(format!("{}\n", Control::Error { message: e }.encode()).as_bytes())
                .await;
            return;
        }
    };

    let reply = match cmd {
        Control::Hello { .. } => {
            // The deadman line. Ping until the node goes away — see the comment
            // in `control.rs` for why silence, rather than a closed socket, is
            // the signal a node watches for.
            let ping = format!("{}\n", Control::Ping.encode());
            let mut ticker = tokio::time::interval(DEADMAN_PING);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if write_half.write_all(ping.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                    line = lines.next_line() => {
                        if !matches!(line, Ok(Some(_))) {
                            return;
                        }
                    }
                }
            }
        }
        Control::Kill { node } => match kill_node(node, &shared) {
            Ok(()) => Control::Ok {
                message: format!("killed n{node}"),
            },
            Err(e) => Control::Error { message: e },
        },
        Control::Start { node } => match start_node(&opts, node, &shared, &line_tx) {
            Ok(()) => Control::Ok {
                message: format!("started n{node}"),
            },
            Err(e) => Control::Error { message: e },
        },
        Control::Running => {
            let mut running: Vec<NodeId> = shared
                .children
                .lock()
                .map(|c| c.keys().copied().collect())
                .unwrap_or_default();
            running.sort_unstable();
            Control::Nodes { running }
        }
        Control::Down => {
            shutdown.notify_one();
            Control::Ok {
                message: "shutting down".into(),
            }
        }
        Control::Ping | Control::Ok { .. } | Control::Error { .. } | Control::Nodes { .. } => {
            Control::Error {
                message: "that is a reply, not a command".into(),
            }
        }
    };
    let _ = write_half
        .write_all(format!("{}\n", reply.encode()).as_bytes())
        .await;
}

fn node_exe() -> Result<PathBuf, String> {
    // The node binary is built next to this one. Looking there rather than on
    // PATH means `cargo run -- up` works with nothing installed.
    let me = std::env::current_exe().map_err(|e| format!("{e}"))?;
    let exe = me.with_file_name(format!("dkv-node{}", std::env::consts::EXE_SUFFIX));
    if !exe.exists() {
        return Err(format!(
            "node binary not found at {} — run `cargo build` first",
            exe.display()
        ));
    }
    Ok(exe)
}

fn start_node(
    opts: &NodeOptions,
    id: NodeId,
    shared: &Arc<Shared>,
    line_tx: &mpsc::Sender<String>,
) -> Result<(), String> {
    if !CLUSTER.contains(&id) {
        return Err(format!("no such node n{id} (nodes are 1, 2, 3)"));
    }
    if shared
        .children
        .lock()
        .map(|c| c.contains_key(&id))
        .unwrap_or(false)
    {
        return Err(format!("n{id} is already running"));
    }

    let exe = node_exe()?;
    let mut cmd = Command::new(exe);
    cmd.arg("--id")
        .arg(id.to_string())
        .arg("--port-base")
        .arg(opts.port_base.to_string())
        .arg("--data")
        .arg(&opts.data_dir)
        .arg("--election-min")
        .arg(opts.election_min.to_string())
        .arg("--election-max")
        .arg(opts.election_max.to_string())
        .arg("--heartbeat")
        .arg(opts.heartbeat.to_string())
        .arg("--fsync")
        .arg(&opts.fsync)
        .arg("--snapshot-threshold")
        .arg(opts.snapshot_threshold.to_string())
        .arg("--log")
        .arg(&opts.log)
        .arg("--supervisor")
        .arg(opts.port_base.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null())
        .kill_on_drop(true);
    if !opts.color {
        cmd.arg("--no-color");
    }
    #[cfg(windows)]
    cmd.creation_flags(CREATE_NEW_PROCESS_GROUP);

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("cannot spawn n{id}: {e}"))?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    if let Ok(mut killed) = shared.killed.lock() {
        killed.remove(&id);
    }
    if let Ok(mut children) = shared.children.lock() {
        children.insert(id, child);
    }

    let color = opts.color;
    let tx = line_tx.clone();
    if let Some(err) = stderr {
        let tx_err = tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(err).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let _ = tx_err
                    .send(format_line(
                        id,
                        Cat::Fail,
                        &format!("stderr: {line}"),
                        color,
                    ))
                    .await;
            }
        });
    }
    let shared = Arc::clone(shared);
    tokio::spawn(async move {
        if let Some(out) = stdout {
            let mut lines = BufReader::new(out).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if tx.send(line).await.is_err() {
                    return;
                }
            }
        }
        // stdout closed: the process is gone. Say so in its own colour, in the
        // same column layout, so the death reads as part of the story.
        let was_killed = shared
            .killed
            .lock()
            .map(|k| k.contains(&id))
            .unwrap_or(false);
        if let Ok(mut children) = shared.children.lock() {
            children.remove(&id);
        }
        let why = if was_killed {
            "process exited (killed)"
        } else {
            "process exited unexpectedly"
        };
        let _ = tx.send(format_line(id, Cat::Fail, why, color)).await;
    });
    Ok(())
}

fn kill_node(id: NodeId, shared: &Arc<Shared>) -> Result<(), String> {
    if let Ok(mut killed) = shared.killed.lock() {
        killed.insert(id);
    }
    let mut children = shared
        .children
        .lock()
        .map_err(|_| "lock poisoned".to_string())?;
    match children.get_mut(&id) {
        Some(child) => {
            child
                .start_kill()
                .map_err(|e| format!("cannot kill n{id}: {e}"))?;
            Ok(())
        }
        None => Err(format!("n{id} is not running")),
    }
}

/// One control request from another terminal. `Err` with a connection failure
/// means no supervisor is running — the callers turn that into a helpful hint.
pub async fn control(port_base: u16, cmd: Control) -> Result<Control, String> {
    let addr = format!("127.0.0.1:{port_base}");
    let work = async {
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|_| format!("no supervisor on {addr} — start one with `dkv up`"))?;
        let (read_half, mut write_half) = stream.into_split();
        write_half
            .write_all(format!("{}\n", cmd.encode()).as_bytes())
            .await
            .map_err(|e| format!("{e}"))?;
        let mut lines = BufReader::new(read_half).lines();
        match lines.next_line().await {
            Ok(Some(line)) => Control::decode(&line),
            Ok(None) => Err("supervisor closed the connection".into()),
            Err(e) => Err(format!("{e}")),
        }
    };
    tokio::time::timeout(Duration::from_secs(3), work)
        .await
        .map_err(|_| "supervisor did not answer".to_string())?
}

pub async fn supervisor_is_running(port_base: u16) -> bool {
    TcpStream::connect(format!("127.0.0.1:{port_base}"))
        .await
        .is_ok()
}
