//! TCP plumbing: one listener, one long-lived task per peer, and the fault
//! filter that sits between them.
//!
//! The rule that shapes this file: **the consensus loop never awaits the
//! network.** It hands a message to a peer task's channel and moves on. A leader
//! that blocked on a slow follower would stop sending heartbeats to the healthy
//! ones and get itself voted out — a self-inflicted failure that looks exactly
//! like a network problem when you are reading logs at 2am.

use crate::config::Config;
use crate::faults::{Faults, Verdict};
use crate::logfmt::{Cat, Logger};
use crate::proto::Message;
use crate::types::{ClientError, NodeId};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const RECONNECT_MIN: Duration = Duration::from_millis(200);
const RECONNECT_MAX: Duration = Duration::from_millis(2000);
/// Outbound queue depth per peer. Small on purpose: if a peer is so far behind
/// that this fills up, dropping is the honest outcome — Raft will retransmit,
/// and an unbounded queue would just turn a network problem into a memory one.
const PEER_QUEUE: usize = 256;

#[derive(Debug)]
pub enum NodeEvent {
    /// Something arrived on a connection this node accepted. `reply` writes back
    /// on that same connection — for a client request it may be held for as long
    /// as the write takes to commit.
    Incoming {
        msg: Message,
        reply: mpsc::Sender<Message>,
    },
    /// A reply arrived on our outbound connection to a peer.
    FromPeer {
        peer: NodeId,
        msg: Message,
    },
    PeerUp(NodeId),
    PeerDown(NodeId),
    /// A snapshot finished being written on the blocking pool.
    SnapshotWritten {
        last_included_index: u64,
        bytes: u64,
        error: Option<String>,
    },
}

/// Accepts connections forever. Peer RPCs and client requests share one port, so
/// the protocol you read with netcat is the same one the cluster uses.
pub async fn spawn_listener(
    cfg: Config,
    events: mpsc::Sender<NodeEvent>,
    faults: Arc<Mutex<Faults>>,
    logger: Arc<Logger>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(cfg.my_addr()).await?;
    logger.say(Cat::Rcvr, format!("listening on {}", cfg.my_addr()));
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let events = events.clone();
                    let faults = Arc::clone(&faults);
                    let logger = Arc::clone(&logger);
                    tokio::spawn(serve_connection(stream, events, faults, logger));
                }
                Err(e) => {
                    logger.say(Cat::Fail, format!("accept failed: {e}"));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok(())
}

async fn serve_connection(
    stream: TcpStream,
    events: mpsc::Sender<NodeEvent>,
    faults: Arc<Mutex<Faults>>,
    logger: Arc<Logger>,
) {
    let _ = stream.set_nodelay(true);
    let (read_half, mut write_half) = stream.into_split();
    let (reply_tx, mut reply_rx) = mpsc::channel::<Message>(64);

    tokio::spawn(async move {
        while let Some(msg) = reply_rx.recv().await {
            let line = format!("{}\n", msg.encode());
            if write_half.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let mut lines = BufReader::new(read_half).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let msg = match Message::decode(&line) {
            Ok(m) => m,
            Err(e) => {
                logger.detail(Cat::Fail, format!("undecodable frame: {e}"));
                continue;
            }
        };
        logger.trace(Cat::Repl, format!("recv {}", line));

        // Inbound faults apply only to peer traffic. Client and admin traffic
        // always gets through, deliberately: being able to ask a partitioned
        // node what it believes is the entire point of partitioning it.
        if let Some(sender) = msg.sender()
            && let Ok(f) = faults.lock()
            && let Verdict::Dropped { reason } = f.decide(sender)
        {
            logger.detail(
                Cat::Fail,
                format!("inbound {} from n{sender} {reason}", msg.label()),
            );
            continue;
        }

        if events
            .send(NodeEvent::Incoming {
                msg,
                reply: reply_tx.clone(),
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Owns the outbound connection to one peer, including reconnects and the
/// per-link fault filter. Returns the channel the consensus loop pushes into.
pub fn spawn_peer(
    cfg: Config,
    peer: NodeId,
    events: mpsc::Sender<NodeEvent>,
    faults: Arc<Mutex<Faults>>,
    logger: Arc<Logger>,
) -> mpsc::Sender<Message> {
    let (tx, mut rx) = mpsc::channel::<Message>(PEER_QUEUE);
    let addr = cfg.addr(peer);

    tokio::spawn(async move {
        let mut backoff = RECONNECT_MIN;
        loop {
            let stream = match TcpStream::connect(&addr).await {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    logger.detail(Cat::Rcvr, format!("connected to n{peer}"));
                    let _ = events.send(NodeEvent::PeerUp(peer)).await;
                    backoff = RECONNECT_MIN;
                    s
                }
                Err(_) => {
                    // Drain anything queued while the peer is unreachable rather
                    // than delivering a stale burst on reconnect: Raft resends
                    // whatever still matters on the next heartbeat.
                    while rx.try_recv().is_ok() {}
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                    continue;
                }
            };

            let (read_half, mut write_half) = stream.into_split();
            let reader_events = events.clone();
            let reader_faults = Arc::clone(&faults);
            let reader_logger = Arc::clone(&logger);
            let mut reader = tokio::spawn(async move {
                let mut lines = BufReader::new(read_half).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let Ok(msg) = Message::decode(&line) else {
                        continue;
                    };
                    if let Ok(f) = reader_faults.lock()
                        && let Verdict::Dropped { reason } = f.decide(peer)
                    {
                        reader_logger.detail(
                            Cat::Fail,
                            format!("inbound {} from n{peer} {reason}", msg.label()),
                        );
                        continue;
                    }
                    if reader_events
                        .send(NodeEvent::FromPeer { peer, msg })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });

            let disconnected = loop {
                tokio::select! {
                    // The reader ending means the peer closed the connection.
                    _ = &mut reader => break true,
                    outgoing = rx.recv() => {
                        let Some(msg) = outgoing else { return };
                        let verdict = match faults.lock() {
                            Ok(f) => f.decide(peer),
                            Err(_) => Verdict::Deliver { delay: Duration::ZERO },
                        };
                        match verdict {
                            Verdict::Dropped { reason } => {
                                logger.detail(
                                    Cat::Fail,
                                    format!("outbound {} to n{peer} {reason}", msg.label()),
                                );
                                continue;
                            }
                            Verdict::Deliver { delay } => {
                                // Sleeping here, in the peer's own task, is what
                                // makes injected latency behave like a slow link
                                // instead of a slow cluster.
                                if !delay.is_zero() {
                                    tokio::time::sleep(delay).await;
                                }
                            }
                        }
                        let line = format!("{}\n", msg.encode());
                        logger.trace(Cat::Repl, format!("send n{peer} {line}"));
                        if write_half.write_all(line.as_bytes()).await.is_err() {
                            break true;
                        }
                    }
                }
            };

            if disconnected {
                reader.abort();
                let _ = events.send(NodeEvent::PeerDown(peer)).await;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
        }
    });

    tx
}

/// Reaching a node that is simply not there must be cheap. Without a separate,
/// short connect deadline a dead node costs seconds per command — Windows does
/// not always refuse a connection to a closed local port promptly — and every
/// `dkv set` while one node is down feels broken even though the cluster is
/// perfectly healthy.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(400);

/// One request, one reply, then close. Used by the CLI — it has no state to keep
/// and reconnecting per command keeps `dkv` a plain short-lived process.
///
/// `timeout` bounds the whole exchange, and has to be generous: a write parked
/// behind an unreachable majority legitimately takes seconds before the node
/// gives up on it.
pub async fn request(addr: &str, msg: Message, timeout: Duration) -> Result<Message, ClientError> {
    let work = async {
        let stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(ClientError::Transport(format!("{addr}: {e}"))),
            Err(_) => return Err(ClientError::Transport(format!("{addr}: unreachable"))),
        };
        let _ = stream.set_nodelay(true);
        let (read_half, mut write_half) = stream.into_split();
        let line = format!("{}\n", msg.encode());
        write_half
            .write_all(line.as_bytes())
            .await
            .map_err(|e| ClientError::Transport(format!("{e}")))?;
        let mut lines = BufReader::new(read_half).lines();
        match lines.next_line().await {
            Ok(Some(reply)) => Message::decode(&reply).map_err(ClientError::Transport),
            Ok(None) => Err(ClientError::Transport("connection closed".into())),
            Err(e) => Err(ClientError::Transport(format!("{e}"))),
        }
    };
    match tokio::time::timeout(timeout, work).await {
        Ok(result) => result,
        Err(_) => Err(ClientError::Timeout),
    }
}
