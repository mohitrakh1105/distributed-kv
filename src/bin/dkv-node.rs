//! One node of the cluster.
//!
//! Usually started for you by `dkv up`, but it is a plain process and running
//! three of them by hand in three terminals works just as well — and is worth
//! doing once, to see that there is no magic holding the cluster together.

use clap::Parser;
use dkv::cli::supervise::DEADMAN_TIMEOUT;
use dkv::config::{Config, FsyncMode, LogLevel};
use dkv::control::Control;
use dkv::faults::Faults;
use dkv::logfmt::{Cat, Logger};
use dkv::proto::Message;
use dkv::raft::node::RaftNode;
use dkv::transport::{self, NodeEvent};
use dkv::types::NodeId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Parser, Debug)]
#[command(name = "dkv-node", about = "One node of the dkv cluster")]
struct Args {
    /// Which node this is: 1, 2 or 3.
    #[arg(long)]
    id: NodeId,

    /// Node N listens on port_base + N.
    #[arg(long, default_value_t = dkv::config::DEFAULT_PORT_BASE)]
    port_base: u16,

    #[arg(long, default_value = ".dkv")]
    data: PathBuf,

    #[arg(long, default_value_t = 1000)]
    election_min: u64,

    #[arg(long, default_value_t = 2000)]
    election_max: u64,

    #[arg(long, default_value_t = 300)]
    heartbeat: u64,

    /// always | batch | never
    #[arg(long, default_value = "always")]
    fsync: String,

    /// Compact once the log holds more than this many entries.
    #[arg(long, default_value_t = 64)]
    snapshot_threshold: u64,

    /// normal | verbose | trace
    #[arg(long, default_value = "normal")]
    log: String,

    #[arg(long)]
    no_color: bool,

    /// Supervisor control port to hold a deadman connection to.
    #[arg(long)]
    supervisor: Option<u16>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let fsync: FsyncMode = match args.fsync.parse() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let level: LogLevel = match args.log.parse() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };

    let mut cfg = Config::new(args.id, args.data);
    cfg.port_base = args.port_base;
    cfg.election_min_ms = args.election_min;
    cfg.election_max_ms = args.election_max;
    cfg.heartbeat_ms = args.heartbeat;
    cfg.fsync = fsync;
    cfg.snapshot_threshold = args.snapshot_threshold;
    cfg.log_level = level;
    cfg.color = !args.no_color;
    cfg.supervisor_port = args.supervisor;

    if let Err(e) = std::fs::create_dir_all(cfg.node_dir()) {
        eprintln!("cannot create {}: {e}", cfg.node_dir().display());
        std::process::exit(1);
    }

    let logger = Arc::new(
        Logger::new(cfg.id, cfg.log_level, cfg.color).with_file(&cfg.node_dir().join("node.log")),
    );

    if let Some(port) = cfg.supervisor_port {
        hold_deadman_line(port, cfg.id);
    }

    let faults = Arc::new(Mutex::new(Faults::new()));
    let (events_tx, events_rx) = tokio::sync::mpsc::channel::<NodeEvent>(1024);

    if let Err(e) = transport::spawn_listener(
        cfg.clone(),
        events_tx.clone(),
        Arc::clone(&faults),
        Arc::clone(&logger),
    )
    .await
    {
        logger.say(Cat::Fail, format!("cannot bind {}: {e}", cfg.my_addr()));
        std::process::exit(1);
    }

    let mut peers: HashMap<NodeId, tokio::sync::mpsc::Sender<Message>> = HashMap::new();
    for peer in cfg.peers() {
        peers.insert(
            peer,
            transport::spawn_peer(
                cfg.clone(),
                peer,
                events_tx.clone(),
                Arc::clone(&faults),
                Arc::clone(&logger),
            ),
        );
    }

    let node = match RaftNode::open(
        cfg.clone(),
        Arc::clone(&logger),
        faults,
        events_tx,
        events_rx,
        peers,
    ) {
        Ok(n) => n,
        Err(e) => {
            logger.say(Cat::Fail, format!("cannot start: {e}"));
            std::process::exit(1);
        }
    };

    node.run().await;
}

/// Watches the supervisor's heartbeat and exits when it stops, so a dead
/// supervisor never leaves three orphaned nodes holding their ports.
///
/// Two things about this are deliberate and both were learned the hard way.
///
/// **It waits for a missing ping, not a closed socket.** Sockets are inheritable
/// handles on Windows, so this very process holds a duplicate of the
/// supervisor's end of the connection. Kill the supervisor and the socket stays
/// open, held alive by the children that were supposed to notice it died.
///
/// **It runs on its own OS thread, not on the tokio runtime, and never logs.**
/// When the supervisor dies, nothing drains this node's stdout pipe. It fills,
/// and then every `println!` blocks forever — including the ones on tokio worker
/// threads, which takes the whole runtime down with it. A watchdog scheduled on
/// the runtime it is watching is not a watchdog. A plain thread with a socket
/// read timeout cannot be starved this way, and it stays out of stdout entirely
/// so it can never block on the thing it exists to escape.
fn hold_deadman_line(port: u16, id: NodeId) {
    std::thread::spawn(move || {
        let addr = format!("127.0.0.1:{port}");
        let Ok(mut stream) = std::net::TcpStream::connect(&addr) else {
            return;
        };
        let hello = format!("{}\n", Control::Hello { node: id }.encode());
        if std::io::Write::write_all(&mut stream, hello.as_bytes()).is_err() {
            return;
        }
        if stream.set_read_timeout(Some(DEADMAN_TIMEOUT)).is_err() {
            return;
        }
        let mut buf = [0u8; 256];
        loop {
            match std::io::Read::read(&mut stream, &mut buf) {
                Ok(0) => break,    // supervisor closed the line
                Ok(_) => continue, // a ping: still alive
                Err(_) => break,   // timed out waiting, or the socket broke
            }
        }
        std::process::exit(0);
    });
}
