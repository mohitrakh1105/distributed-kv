# dkv

A three-node distributed key-value store in Rust, built to be broken on purpose.

Real Raft — leader election, log replication, snapshots, crash recovery — running as three separate OS processes over TCP, with runtime switches for killing nodes, cutting network links and dropping packets while it runs.

It is a learning project. Every distributed-systems problem it demonstrates has a command that demonstrates it.

**[LEARNING.md](LEARNING.md) is the main document** — architecture, how a write travels, the Raft rules that matter, and a list of experiments with the output to expect. This file is the command reference.

---

## Run it

```bash
cargo build --release
```

Terminal one — the cluster and its logs:

```bash
dkv up
```

Terminal two — everything else:

```bash
dkv set name mohit
dkv get name
dkv status
```

Then the one worth seeing first:

```bash
dkv demo partition
```

A leader gets isolated, accepts a write, never commits it, is replaced, and has that write deleted from its log when it rejoins — narrated step by step.

On Windows, `.\scripts\dkv.ps1` runs the binary without installing anything, and `.\scripts\lab.ps1` opens the cluster and the live dashboard as two Windows Terminal panes.

Nodes listen on 6101/6102/6103, supervisor on 6100. Change with `--port-base`.

---

## Commands

### Cluster lifecycle — needs `dkv up` running

| Command | What it does |
|---|---|
| `dkv up` | Start all three nodes, stream their logs here. Ctrl-C stops them. |
| `dkv up --fresh` | Same, after deleting all existing data. |
| `dkv up --fsync never` | `always` (default), `batch` or `never` — durability vs speed. |
| `dkv up --log verbose` | `normal` (the story), `verbose` (+ every AppendEntries), `trace` (+ raw JSON). |
| `dkv down` | Stop the cluster. |
| `dkv kill 3` | Hard-kill node 3 — TerminateProcess, no flush. Exercises crash recovery. |
| `dkv start 3` | Restart it. |

`up` also takes `--heartbeat`, `--election-min`, `--election-max` and `--snapshot-threshold` if you want to make elections faster or compaction more frequent.

### Data

| Command | What it does |
|---|---|
| `dkv set k v` | Write. Follows leader redirects automatically. |
| `dkv get k` | Linearizable read — the leader confirms with a majority first. |
| `dkv get k --stale` | Local read, no coordination. Fast, possibly out of date. |
| `dkv del k` | Delete. |
| `dkv set k v -n 2` | Send to node 2 specifically, no redirect. Useful on a partitioned node. |

### Looking inside

| Command | What it does |
|---|---|
| `dkv status` | Role, term, commit index, key count and state hash per node. |
| `dkv status --json` | The same, for scripts. |
| `dkv inspect log -n 1` | Node 1's Raft log, marked committed or UNCOMMITTED. |
| `dkv inspect sm -n 1` | Its state machine contents. |
| `dkv inspect snap -n 1` | Its snapshot metadata. |
| `dkv verify` | Do all nodes hold identical state? Compares Merkle roots. |
| `dkv watch` | Live dashboard, 4 fps. `--narrow` for small terminals. |
| `dkv bench --ops 300` | Write load with latency percentiles. |

### Breaking things

| Command | What it does |
|---|---|
| `dkv part 1` | Cut node 1 off from both peers. Sockets stay open; messages vanish. |
| `dkv part 1 3` | Cut just the link between 1 and 3. |
| `dkv heal` | Restore every link and clear all faults. |
| `dkv fault --drop 0.3` | Drop 30% of messages on every link. |
| `dkv fault -n 3 --delay 400` | Add 400ms to node 3's links. |
| `dkv fault --clear` | Clear injected faults (without healing partitions). |

### Demos

```bash
dkv demo                 # list them
dkv demo partition       # start here
dkv demo <name> --auto   # advance on a timer instead of pressing Enter
```

`election`, `replication`, `partition`, `flaky`, `durability`, `snapshot`, `reads`, `dedup`.

A demo attaches to a running cluster if there is one, and otherwise starts and stops its own.

---

## Reading the logs

```text
12:04:31.182 [n1] CLNT SET name=mohit -> log idx 7 term 3
12:04:31.184 [n1] REPL append idx 7 -> n2, n3
12:04:31.193 [n1] CLNT committed idx 7 (quorum 2/3, 11ms) OK
------------ n2  FOLLOWER -> CANDIDATE  term 4 ------------
```

`HH:MM:SS.mmm [node] CATEGORY message`, timestamps in UTC. Categories: `CLNT` client requests, `REPL` replication, `ELEC` elections, `ROLE` role changes (drawn as a rule), `FAIL` failures, `RCVR` recoveries, `SNAP` snapshots, `DISK` disk, `ADMN` admin, `BEAT` heartbeats.

At `normal` level heartbeats are only logged when their status *changes*, so a healthy cluster is quiet and the interesting lines are the only lines. `--log verbose` shows the mechanism; `--log trace` shows every frame.

Each node also writes an uncoloured copy to `.dkv/node-N/node.log`.

---

## Layout

```text
src/
  proto.rs          the wire protocol — read this first
  raft/node.rs      the consensus loop: all of Raft's Figure 2
  raft/log_store.rs the durable log, CRC per record, torn-tail recovery
  raft/snapshot.rs  compaction and InstallSnapshot
  statemachine.rs   the map the log produces, plus the dedup table
  transport.rs      TCP, per-peer tasks, retries
  faults.rs         fake partitions, packet loss, latency
  merkle.rs         the convergence checker behind `dkv verify`
  cli/              supervisor, demos, dashboard, bench
tests/cluster.rs    integration tests against three real processes
```

Data lives in `.dkv/node-N/`: `raft.log`, `raft.state`, `snapshot.json`, `node.log`. All of it is readable by hand, which is the point — `cat .dkv/node-1/snapshot.json` is a valid way to learn what a snapshot is.

## Tests

```bash
cargo test
```

Unit tests live beside the code they test. `tests/cluster.rs` starts real three-node clusters and does unpleasant things to them: partitions the leader mid-write, kills every node at once, drops 30% of packets, replays a retried request. Each one targets a specific Raft bug that is easy to write and hard to notice.
