//! `dkv` — the one command you drive the cluster with.
//!
//! Two kinds of subcommand. `up`, `down`, `kill` and `start` are about
//! processes, and need the supervisor. Everything else talks straight to a node
//! over the same TCP port the cluster uses internally, so it works whether the
//! nodes were started by `dkv up` or by hand in three terminals.

use clap::{Parser, Subcommand};
use dkv::cli::commands::{Cluster, new_client_id};
use dkv::cli::supervise::{NodeOptions, Supervisor, control, supervisor_is_running};
use dkv::cli::{bench, demo, watch};
use dkv::control::Control;
use dkv::logfmt::color_enabled;
use dkv::proto::{AdminCmd, AdminReply, Op};
use dkv::types::NodeId;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "dkv",
    about = "A three-node Raft key-value store you can break on purpose",
    after_help = "Start here:\n  dkv up                 (leave it running in this terminal)\n  dkv demo partition     (in a second terminal — the one worth seeing first)\n\nEverything else: dkv set/get/del, status, inspect, kill, part, fault, verify, watch, bench."
)]
struct Cli {
    /// Nodes listen on port_base+1..+3; the supervisor owns port_base itself.
    #[arg(long, global = true, default_value_t = dkv::config::DEFAULT_PORT_BASE)]
    port_base: u16,

    #[arg(long, global = true)]
    no_color: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start all three nodes and stream their logs here. Ctrl-C stops them.
    Up {
        /// Delete all existing data first.
        #[arg(long)]
        fresh: bool,
        #[arg(long, default_value = ".dkv")]
        data: PathBuf,
        /// always | batch | never
        #[arg(long, default_value = "always")]
        fsync: String,
        /// normal | verbose | trace
        #[arg(long, default_value = "normal")]
        log: String,
        #[arg(long, default_value_t = 300)]
        heartbeat: u64,
        #[arg(long, default_value_t = 1000)]
        election_min: u64,
        #[arg(long, default_value_t = 2000)]
        election_max: u64,
        #[arg(long, default_value_t = 64)]
        snapshot_threshold: u64,
    },
    /// Stop the cluster started by `dkv up`.
    Down,
    /// Hard-kill one node (TerminateProcess — no flush, no cleanup).
    Kill { node: NodeId },
    /// Restart a node that was killed.
    Start { node: NodeId },

    /// Write a key.
    Set {
        key: String,
        value: String,
        /// Send to this node only, instead of following leader hints.
        #[arg(short = 'n', long)]
        node: Option<NodeId>,
    },
    /// Read a key.
    Get {
        key: String,
        #[arg(short = 'n', long)]
        node: Option<NodeId>,
        /// Read this node's local state directly: fast, and possibly out of date.
        #[arg(long)]
        stale: bool,
    },
    /// Delete a key.
    Del {
        key: String,
        #[arg(short = 'n', long)]
        node: Option<NodeId>,
    },

    /// One line per node: role, term, commit index, key count, state hash.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Look inside a node: `log`, `sm` (state machine) or `snap` (snapshot).
    Inspect {
        #[arg(default_value = "log")]
        what: String,
        #[arg(short = 'n', long, default_value_t = 1)]
        node: NodeId,
        #[arg(long)]
        from: Option<u64>,
        #[arg(long)]
        to: Option<u64>,
    },

    /// Cut the link between two nodes — or isolate one from both peers.
    Part { a: NodeId, b: Option<NodeId> },
    /// Restore links: one node's, or all of them.
    Heal { node: Option<NodeId> },
    /// Inject packet loss and latency without a full partition.
    Fault {
        #[arg(short = 'n', long)]
        node: Option<NodeId>,
        /// 0.0 to 1.0
        #[arg(long)]
        drop: Option<f64>,
        /// Added one-way delay, in milliseconds.
        #[arg(long)]
        delay: Option<u64>,
        #[arg(long)]
        clear: bool,
    },

    /// Check that every node applied the same log and holds identical state.
    Verify {
        /// Keep checking for up to this many seconds before giving a verdict.
        #[arg(long, default_value_t = 5)]
        wait: u64,
    },
    /// Write load, with latency percentiles.
    Bench {
        #[arg(long, default_value_t = 200)]
        ops: usize,
        #[arg(long, default_value_t = 8)]
        conns: usize,
        #[arg(long, default_value_t = 32)]
        value_bytes: usize,
    },
    /// Live dashboard of all three nodes.
    Watch {
        #[arg(long, default_value_t = 4)]
        fps: u32,
        /// Stack the nodes vertically for a narrow terminal.
        #[arg(long)]
        narrow: bool,
    },
    /// Run a narrated scenario. With no name, lists them.
    Demo {
        name: Option<String>,
        /// Advance on a timer instead of waiting for Enter.
        #[arg(long)]
        auto: bool,
        /// Speed multiplier for --auto.
        #[arg(long, default_value_t = 1.0)]
        speed: f64,
        /// Wipe data first when the demo has to start its own cluster.
        #[arg(long)]
        fresh: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let color = color_enabled(!cli.no_color);
    let cluster = Cluster {
        port_base: cli.port_base,
        color,
    };

    match cli.command {
        Command::Up {
            fresh,
            data,
            fsync,
            log,
            heartbeat,
            election_min,
            election_max,
            snapshot_threshold,
        } => {
            let opts = NodeOptions {
                port_base: cli.port_base,
                data_dir: data,
                election_min,
                election_max,
                heartbeat,
                fsync,
                snapshot_threshold,
                log,
                color,
            };
            run_up(cluster, opts, fresh).await;
        }
        Command::Down => match control(cli.port_base, Control::Down).await {
            Ok(Control::Ok { message }) => println!("{}", cluster.good(&message)),
            Ok(other) => println!("{other:?}"),
            Err(e) => {
                println!("{}", cluster.warn(&e));
            }
        },
        Command::Kill { node } => {
            lifecycle(&cluster, Control::Kill { node }).await;
        }
        Command::Start { node } => {
            lifecycle(&cluster, Control::Start { node }).await;
        }

        Command::Set { key, value, node } => {
            let outcome = cluster
                .send_op(
                    node,
                    new_client_id(),
                    1,
                    Op::Set {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )
                .await;
            print_op(&cluster, &format!("SET {key}={value}"), outcome);
        }
        Command::Get { key, node, stale } => {
            let outcome = cluster
                .send_op(
                    node,
                    new_client_id(),
                    1,
                    Op::Get {
                        key: key.clone(),
                        stale,
                    },
                )
                .await;
            print_op(&cluster, &format!("GET {key}"), outcome);
        }
        Command::Del { key, node } => {
            let outcome = cluster
                .send_op(node, new_client_id(), 1, Op::Delete { key: key.clone() })
                .await;
            print_op(&cluster, &format!("DEL {key}"), outcome);
        }

        Command::Status { json } => {
            let statuses = cluster.status_all().await;
            if json {
                let live: Vec<_> = statuses.iter().filter_map(|(_, s)| s.clone()).collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&live).unwrap_or_else(|e| format!("{e}"))
                );
            } else {
                cluster.print_status_table(&statuses);
            }
        }
        Command::Inspect {
            what,
            node,
            from,
            to,
        } => {
            let cmd = match what.as_str() {
                "log" => AdminCmd::InspectLog { from, to },
                "sm" | "state" => AdminCmd::InspectState,
                "snap" | "snapshot" => AdminCmd::InspectSnapshot,
                other => {
                    println!(
                        "{}",
                        cluster.bad(&format!("unknown target '{other}' (log | sm | snap)"))
                    );
                    return;
                }
            };
            match cluster.admin(node, cmd).await {
                Ok(reply) => print_inspect(&cluster, node, &reply),
                Err(e) => println!("{}", cluster.bad(&format!("n{node}: {e}"))),
            }
        }

        Command::Part { a, b } => {
            let peers: Vec<NodeId> = match b {
                Some(b) => vec![b],
                None => cluster.nodes().into_iter().filter(|n| *n != a).collect(),
            };
            for peer in peers {
                set_partition(&cluster, a, peer, true).await;
            }
        }
        Command::Heal { node } => {
            let pairs: Vec<(NodeId, NodeId)> = match node {
                Some(n) => cluster
                    .nodes()
                    .into_iter()
                    .filter(|o| *o != n)
                    .map(|o| (n, o))
                    .collect(),
                None => {
                    let mut all = Vec::new();
                    for a in cluster.nodes() {
                        for b in cluster.nodes() {
                            if a < b {
                                all.push((a, b));
                            }
                        }
                    }
                    all
                }
            };
            for (a, b) in pairs {
                set_partition(&cluster, a, b, false).await;
            }
            for n in cluster.nodes() {
                let _ = cluster
                    .admin(
                        n,
                        AdminCmd::Fault {
                            peer: None,
                            drop_rate: None,
                            delay_ms: None,
                            clear: true,
                        },
                    )
                    .await;
            }
            println!("{}", cluster.good("links restored"));
        }
        Command::Fault {
            node,
            drop,
            delay,
            clear,
        } => {
            let targets: Vec<NodeId> = match node {
                Some(n) => vec![n],
                None => cluster.nodes().to_vec(),
            };
            for n in targets {
                match cluster
                    .admin(
                        n,
                        AdminCmd::Fault {
                            peer: None,
                            drop_rate: drop,
                            delay_ms: delay,
                            clear,
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        let what = if clear {
                            "faults cleared".to_string()
                        } else {
                            format!(
                                "drop {:.0}%, +{}ms on every link",
                                drop.unwrap_or(0.0) * 100.0,
                                delay.unwrap_or(0)
                            )
                        };
                        println!("{} {}", cluster.tag(n), cluster.warn(&what));
                    }
                    Err(e) => println!("{} {}", cluster.tag(n), cluster.bad(&format!("{e}"))),
                }
            }
        }

        Command::Verify { wait } => {
            let ok = cluster.verify(Duration::from_secs(wait)).await;
            if !ok {
                std::process::exit(1);
            }
        }
        Command::Bench {
            ops,
            conns,
            value_bytes,
        } => bench::run(cluster, ops, conns, value_bytes).await,
        Command::Watch { fps, narrow } => watch::run(cluster, fps, narrow).await,
        Command::Demo {
            name,
            auto,
            speed,
            fresh,
        } => run_demo(cluster, cli.port_base, name, auto, speed, fresh).await,
    }
}

async fn run_up(cluster: Cluster, opts: NodeOptions, fresh: bool) {
    let mut sup = match Supervisor::start(opts, fresh).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{}", cluster.bad(&e));
            std::process::exit(1);
        }
    };
    println!(
        "{}",
        cluster.bold(&format!(
            "cluster up on ports {}..{} — Ctrl-C to stop",
            cluster.port_base + 1,
            cluster.port_base + 3
        ))
    );
    println!(
        "{}",
        cluster.dim("in another terminal: dkv set name mohit | dkv status | dkv demo partition")
    );

    let shutdown = std::sync::Arc::clone(&sup.shutdown);
    let mut lines = sup
        .take_lines()
        .expect("the supervisor owns its output until now");
    loop {
        tokio::select! {
            line = lines.recv() => match line {
                Some(line) => println!("{line}"),
                None => break,
            },
            _ = tokio::signal::ctrl_c() => {
                println!("\n{}", cluster.warn("stopping all nodes..."));
                break;
            }
            _ = shutdown.notified() => {
                println!("{}", cluster.warn("shutdown requested; stopping all nodes..."));
                break;
            }
        }
    }
    sup.shutdown().await;
    // Drain whatever the dying nodes managed to say.
    while let Ok(line) = lines.try_recv() {
        println!("{line}");
    }
    println!("{}", cluster.dim("cluster down"));
}

async fn lifecycle(cluster: &Cluster, cmd: Control) {
    match control(cluster.port_base, cmd).await {
        Ok(Control::Ok { message }) => println!("{}", cluster.good(&message)),
        Ok(Control::Error { message }) => println!("{}", cluster.bad(&message)),
        Ok(other) => println!("{other:?}"),
        Err(e) => println!("{}", cluster.bad(&e)),
    }
}

async fn set_partition(cluster: &Cluster, a: NodeId, b: NodeId, on: bool) {
    // Told to both endpoints so the cut is symmetric even if one side is busy.
    let ra = cluster.admin(a, AdminCmd::Partition { peer: b, on }).await;
    let rb = cluster.admin(b, AdminCmd::Partition { peer: a, on }).await;
    let ok = ra.is_ok() || rb.is_ok();
    let what = if on { "CUT" } else { "restored" };
    if ok {
        let line = format!("link n{a} <-> n{b} {what}");
        println!(
            "{}",
            if on {
                cluster.warn(&line)
            } else {
                cluster.good(&line)
            }
        );
    } else {
        println!(
            "{}",
            cluster.bad(&format!(
                "could not reach n{a} or n{b} to change their link"
            ))
        );
    }
}

fn print_op(
    cluster: &Cluster,
    what: &str,
    outcome: Result<dkv::cli::commands::SendOutcome, dkv::types::ClientError>,
) {
    match outcome {
        Ok(o) => cluster.print_outcome(what, &o),
        Err(e) => {
            println!("{} {}", cluster.bad("failed:"), e);
            println!(
                "{}",
                cluster.dim("is the cluster running? start it with `dkv up`")
            );
            std::process::exit(1);
        }
    }
}

fn print_inspect(cluster: &Cluster, node: NodeId, reply: &AdminReply) {
    match reply {
        AdminReply::LogEntries { .. } => cluster.print_log(node, reply),
        AdminReply::State {
            kv,
            last_applied,
            state_hash,
            clients,
        } => {
            println!(
                "{} state machine: {} keys, applied through idx {}, hash {}, {} known clients",
                cluster.tag(node),
                kv.len(),
                last_applied,
                dkv::cli::commands::fmt_hash(*state_hash),
                clients
            );
            for (k, v) in kv {
                println!("  {k} = {v}");
            }
        }
        AdminReply::Snapshot {
            last_included_index,
            last_included_term,
            keys,
            bytes,
        } => {
            if *last_included_index == 0 {
                println!(
                    "{} no snapshot yet — the log is still short enough to keep whole",
                    cluster.tag(node)
                );
            } else {
                println!(
                    "{} snapshot through idx {} (term {}), {} keys, {} bytes on disk",
                    cluster.tag(node),
                    last_included_index,
                    last_included_term,
                    keys,
                    bytes
                );
            }
        }
        AdminReply::Ok => println!("{}", cluster.good("ok")),
        AdminReply::Error { message } => println!("{}", cluster.bad(message)),
        AdminReply::Status(s) => println!("{s:?}"),
    }
}

async fn run_demo(
    cluster: Cluster,
    port_base: u16,
    name: Option<String>,
    auto: bool,
    speed: f64,
    fresh: bool,
) {
    let Some(name) = name else {
        println!("{}", cluster.bold("demos:"));
        for (n, about) in demo::list() {
            println!("  {:<12} {}", cluster.good(n), cluster.dim(about));
        }
        println!();
        println!("{}", cluster.dim("run one with: dkv demo partition"));
        println!(
            "{}",
            cluster.dim("add --auto to advance on a timer instead of pressing Enter")
        );
        return;
    };
    let Some(steps) = demo::script(&name) else {
        println!("{}", cluster.bad(&format!("no demo called '{name}'")));
        println!("{}", cluster.dim("run `dkv demo` to see the list"));
        return;
    };

    let attached = supervisor_is_running(port_base).await;
    let opts = NodeOptions {
        port_base,
        color: cluster.color,
        ..Default::default()
    };

    if attached {
        // A cluster is already running in another terminal; narrate here and let
        // the node logs stay where they are.
        println!(
            "{}",
            cluster.dim("using the cluster already running — its logs are in the other terminal")
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(256);
        tokio::spawn(async move {
            while let Some(line) = rx.recv().await {
                println!("{line}");
            }
        });
        let mut runner = demo::DemoRunner::new(cluster, tx, auto, speed);
        runner.run(&name, steps).await;
        return;
    }

    let mut sup = match Supervisor::start(opts, fresh).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{}", cluster.bad(&e));
            std::process::exit(1);
        }
    };
    let narrate = sup.line_sender();
    let mut lines = sup
        .take_lines()
        .expect("the supervisor owns its output until now");
    // One printer for node logs and narration both, so the story stays ordered.
    let printer = tokio::spawn(async move {
        while let Some(line) = lines.recv().await {
            println!("{line}");
        }
    });

    let mut runner = demo::DemoRunner::new(cluster, narrate, auto, speed);
    runner.run(&name, steps).await;

    sup.shutdown().await;
    printer.abort();
    println!("{}", cluster.dim("cluster stopped"));
}
