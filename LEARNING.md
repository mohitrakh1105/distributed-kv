# LEARNING.md

This is a three-node replicated key-value store built to be broken. It implements Raft — leader election, log replication, snapshots — as three real OS processes talking TCP, with switches for killing nodes, cutting links, and dropping packets while it runs.

Read this once, then stop reading and go run `dkv demo partition`. Everything here is a description of something you can watch happen.

`README.md` has the command reference. This file explains what the commands are showing you.

---

## 1. Sixty seconds

```bash
cargo build --release
```

Terminal one — the cluster, and its logs:

```bash
dkv up
```

Terminal two — everything else:

```bash
dkv set name mohit
dkv get name
dkv status
dkv demo partition
```

Ports are 6101, 6102, 6103, with the supervisor on 6100. (7001-7003 would have been the obvious choice and Docker Desktop already owns them on this machine — hence `--port-base`.)

---

## 2. What the pieces are

```text
        dkv (CLI)                    one process per command, no state
            |
            | TCP, newline-delimited JSON
            v
  +---------+----------+----------+
  |         |          |          |
 n1        n2         n3          each an OS process
  |         |          |
  +----- peer RPCs ----+          AppendEntries, RequestVote, InstallSnapshot

  Inside one node:

    transport.rs   one task per peer socket, plus a listener
         |  events
         v
    raft/node.rs   ONE task owns all consensus state
         |
         +--> raft/log_store.rs   the durable log (CRC per record)
         +--> raft/snapshot.rs    compaction and InstallSnapshot
         +--> statemachine.rs     the map the log produces
         +--> faults.rs           fake partitions, loss, latency
```

The important structural decision: **one task owns every piece of Raft state.** There is no `Arc<Mutex<RaftState>>`. Network sockets live in their own tasks and communicate with the consensus loop over channels. If you are reading `raft/node.rs`, you are the only writer of that data, and the rules in it cannot be violated by something happening concurrently somewhere else.

The client API, the peer RPCs and the admin commands all share one port and one message enum, in `proto.rs`. That file is the protocol documentation. It is JSON on purpose:

```bash
echo '{"type":"Admin","admin":"Status"}' | nc 127.0.0.1 6101
```

---

## 3. The life of a write

`dkv set name mohit` produces this:

```text
1. CLI connects to a node. If it is not the leader it answers NotLeader with a
   hint, and the CLI retries there. You see this as "not leader, retrying".

2. The leader appends the entry to its own log and fsyncs it.
   NOTHING has been promised to anybody yet.

3. The leader sends AppendEntries to both followers.

4. Each follower checks that the entry BEFORE this one matches what it already
   has. If it does, it appends, fsyncs, and acks.

5. When a majority (the leader plus one follower, 2 of 3) has the entry, the
   leader advances its commit index.

6. Committed entries are applied to the state machine in index order.

7. Only now does the client get OK.
```

Step 7 is the whole point. A log entry is not a write. A write that never reached a majority can be — and in `dkv demo partition`, is — deleted afterwards. The client is told OK only once the entry cannot be lost.

The `--w`-style quorum knobs some stores expose do not exist here: Raft's quorum is always a majority. That is not a tuning decision, it is what makes the guarantee work. Two majorities of three nodes always share at least one node, so a new leader always sees everything the old one committed.

## 4. The life of a read

Two kinds, and the difference is the most useful thing in this project.

**Stale** (`dkv get k --stale`): the node answers from its local state machine. No coordination, no waiting. It may be arbitrarily out of date — including "I was the leader until 8 seconds ago and I have not noticed."

**Linearizable** (`dkv get k`, the default): before answering, the leader

1. notes its current commit index,
2. sends a fresh heartbeat round and waits for a majority to answer it,
3. waits until it has applied up to that index,
4. then reads.

Step 2 is the part that seems redundant and is not. Being the leader is a belief, not a fact — a deposed leader believes it for one election timeout longer than it is true. The heartbeat round is how it checks. In `dkv demo reads` the same node answers `before` to a stale read and refuses the linearizable one, in consecutive commands.

The round has a counter on it (`read_round` in the wire protocol). Without it, a heartbeat reply that was already in flight when the read arrived would be miscounted as proof of present leadership — proving only that the node was leader a moment ago, which is exactly the thing being tested.

---

## 5. The Raft rules that actually matter

All of Figure 2 lives in `raft/node.rs`, deliberately in one file: split across an `election.rs` and a `replication.rs`, the step-down rule gets written twice and the two copies drift. Each of these has a comment naming the bug it prevents.

| Rule | What breaks without it |
|---|---|
| Vote only for a candidate whose log is at least as up to date as yours | A node with a truncated log gets elected and erases committed writes |
| A leader commits only entries from **its own** term (Figure 8) | An old entry that merely looks well-replicated gets committed, then overwritten later — a committed value changes |
| Reject AppendEntries unless the previous entry matches; truncate only at a proven mismatch | A delayed duplicate RPC deletes entries the follower already acked |
| Apply only after commit, strictly in index order | Uncommitted writes become visible and then vanish |
| Persist term, vote and entries **before** replying | Crash after voting, restart, vote again in the same term, two leaders |
| Any message with a higher term makes you a follower | Two leaders in one term |
| Reset the election timer only on valid AppendEntries from the leader, or on granting a vote | A stale leader's traffic stops a node ever starting the election it needs |

The tests in `tests/cluster.rs` exist to catch these specifically. `a_committed_value_never_changes_even_across_partitions_and_elections` is the Figure 8 check: it partitions the leader three times and asserts that a value committed at the start is still the same value at the same index every time.

---

## 6. Persistence, and what a hard kill costs

Every entry on disk looks like this:

```text
[u32 payload_len][u32 crc32(payload)][payload: JSON LogEntry]['\n']
```

The trailing newline is not counted in the length. It costs one byte and makes the file greppable, which matters when the point is looking at what got written.

**Torn tails.** `dkv kill 2` is `TerminateProcess`: no unwinding, no flushing. That can leave a half-written record. On restart the node detects it (short read, or a CRC that does not match) and truncates. This is safe for a reason worth internalising: an entry that was never completely written was never acknowledged to anybody either. Nothing that was promised is being dropped.

**fsync.** `dkv up --fsync always|batch|never` changes when the log is forced to disk.

- `always` — every append is on the platter before anyone is told anything. Slowest, and a power cut loses nothing that was acknowledged.
- `batch` — flushed every few milliseconds. Loses only the very tail.
- `never` — the OS decides. Fast, and a hard kill can lose whatever was still in the page cache.

Run `dkv bench` under each and compare. The gap is the price of the promise.

**Snapshots.** A log that only grows is a log that eventually fills the disk. Past `--snapshot-threshold` entries, a node writes its state machine to `snapshot.json` and discards the entries it covers. The ordering is: snapshot durable **first**, then compact. Crash in between and the log still holds the entries the snapshot already contains — recoverable, and `log_store.rs` handles it by skipping them on replay and finishing the compaction. The reverse order would leave a node with neither copy.

Once compacted, a follower that has been away too long cannot be caught up entry by entry, because the entries are gone. The leader ships the whole state machine instead (`InstallSnapshot`). That is `dkv demo snapshot`.

**The dedup table.** A client that retries a write cannot know whether the first attempt happened. Each request carries `(client_id, seq)`, and the state machine remembers the highest sequence it has applied per client. A retry commits a log entry but does not touch the map, and gets the original answer back. Two details that are easy to get wrong: the check lives in `apply`, not at request admission — two leaders in different terms can both admit the same request, so only apply order decides what really happened — and the table travels **inside the snapshot**, or a restored node re-applies the retry.

---

## 7. Failure, partitions and split brain

A failure detector is a timeout with good manners. Here:

- A follower that hears nothing from a leader for its (randomized) election timeout starts an election. Randomization is what stops all three campaigning in lockstep forever.
- A leader that gets no reply from a peer for three heartbeats logs it as unreachable. Only the leader does this: a follower never hears from another follower even in a perfectly healthy cluster, so silence there means nothing.

`dkv part 1` cuts node 1 off from both peers **without closing any socket**. The process stays up and keeps believing whatever it believed. That gap — between "I am the leader" and "anyone can hear me" — is what a partition actually is from inside a process, and dropping the connection instead would give it away immediately.

What you get is two leaders at once, which sounds like the disaster and is not:

```text
n1  LEADER    term 2   commit 4   log 1..5     <- isolated, stuck
n2  LEADER    term 3   commit 5   log 1..5     <- majority side, working
n3  FOLLOWER  term 3   commit 5   log 1..5
```

n1 can append to its own log forever and commit none of it, because commit requires a majority and it can reach nobody. When the partition heals it sees a higher term, steps down, and the entries it accepted but never committed are **truncated** — deleted from the log of the node that accepted them. That is `dkv demo partition`, and the write it loses (`b=2`) is one the client was never told had succeeded.

This is the honest answer to "what about conflicting writes". In a Dynamo-style store, two concurrent writes produce two versions — siblings — and the application resolves them. Raft has no siblings because the log linearizes everything: conflicts appear as log conflicts, and the minority side's entries lose. That is the trade. Raft gives you a single consistent history and refuses writes when it cannot get a majority; Dynamo stays writable and hands you the mess afterwards. Neither is free. What Raft will never do is trade *safety* for liveness: under 60% packet loss it stops making progress, and still never lets a committed value change.

---

## 8. The Rust worth noticing

**One owner, no locks.** `RaftState` lives in exactly one task. Peer sockets are separate tasks that send messages in and get messages out. This is Rust's ownership model used as a concurrency design rather than a borrow-checker obstacle: the invariants in `node.rs` hold because nothing else *can* touch that data.

**Channels as the seam.** `mpsc` for streams of events (peer messages, log lines), `oneshot` for "one answer, later" (a client waiting for its write to commit). A parked client write is a `oneshot::Sender` sitting in a `HashMap<LogIndex, Waiter>` — the reply is sent when the entry applies, from a completely different code path than the one that received the request.

**`tokio::select!`** is the event loop: election timer, heartbeat tick, inbound events, all raced in one place.

**Never `.await` the network from the loop.** `send_to` uses `try_send`, which fails rather than blocks. A leader that awaited a slow follower would stop heartbeating the healthy ones and get itself voted out — a self-inflicted failure that reads exactly like a network problem.

**`spawn_blocking` for disk.** Snapshot writes go to the blocking pool. The payload is cloned synchronously first, so what gets written is a true picture of one index, and only the slow part moves off the loop.

**Errors are plain enums.** `StoreError`, `ClientError` — no `anyhow`, no `thiserror`. Each variant names a real situation (`TornRecord`, `LogDoesNotFollowSnapshot`, `NotLeader`), so handling them is a `match` that the compiler checks.

**Let-chains** (`if let Some(x) = a && let Ok(y) = b`) appear throughout — Rust 2024 edition.

**A Windows lesson, for free.** The supervisor keeps its children from being orphaned with a deadman connection. The obvious design — hold a socket open, exit when it closes — *does not work on Windows*, because sockets are inheritable handles and every child inherits a duplicate of the supervisor's end. Kill the supervisor and the connection stays open, held alive by the very processes that were supposed to notice. So the deadman is a positive signal instead: the supervisor pings every second, and a node that goes four seconds without one exits. Same shape as Raft's own failure detection, and for the same reason.

---

## 9. Experiments

Each of these is a thing to run, followed by what you should see and why. The `demo` ones narrate themselves.

### Start here

```bash
dkv demo partition
```

A leader is isolated, accepts a write, never commits it, gets replaced, and has the write truncated when it rejoins. If you run one thing, run this.

### The rest of the demos

```bash
dkv demo              # lists them
dkv demo election     # leader dies, a new one is elected, the old one rejoins
dkv demo replication  # one write, followed end to end
dkv demo flaky        # 30% loss + 400ms: slower, never wrong
dkv demo durability   # hard kill, torn tail, replay
dkv demo snapshot     # compaction, then catch-up via InstallSnapshot
dkv demo reads        # stale vs linearizable, same node, same second
dkv demo dedup        # the same request twice, applied once
```

Add `--auto` to advance on a timer instead of pressing Enter.

### By hand

**Watch quorum arithmetic.** Kill one node; writes continue. Kill a second; they stop.

```bash
dkv kill 2
dkv set a 1        # still works: 2 of 3 is a majority
dkv kill 3
dkv set b 2        # fails: one node is not a majority
dkv start 2; dkv start 3
dkv verify
```

The failure message is `LostLeadership ... outcome unknown until this node rejoins`, and "unknown" is the honest word: the entry is in a log, and whether it survives depends on what happens next.

**Watch a stale read go stale.**

```bash
dkv part 1
dkv set k after -n 2
dkv get k -n 1 --stale     # the old value, answered instantly
dkv get k -n 1             # refused — it cannot confirm leadership
dkv heal
```

**Make the log tell you the story.**

```bash
dkv inspect log -n 1
dkv inspect sm -n 1
dkv inspect snap -n 1
```

Note index 1 is always a no-op. Every leader appends one when it takes office, so it has an entry from its own term to commit against (the Figure 8 rule). You are looking at the source of truth; the key-value map is just what you get by replaying it.

**Pay for durability.**

```bash
dkv down; dkv up --fsync never
dkv bench --ops 300
# then the same with --fsync always
```

**See commit track the median, not the slowest.**

```bash
dkv fault -n 3 --delay 300
dkv bench --ops 100
```

Latency barely moves: the leader needs one of two followers, and n2 is still fast. Now slow both and watch it change.

**Break it harder than it can take.**

```bash
dkv fault --drop 0.6
dkv status      # terms climbing, elections flapping, nothing committing
dkv fault --clear
dkv verify      # still converged
```

Liveness is gone and safety is intact. That is the whole design in one experiment.

**Watch it live.**

```bash
dkv watch
```

---

## 10. What this deliberately is not

- **No cluster membership changes.** Three fixed nodes. Adding and removing nodes safely (joint consensus) is a genuinely subtle part of Raft and would double this codebase.
- **No sharding.** One Raft group, every key. Real systems run many groups and route by key range.
- **No vector clocks or sibling resolution.** See §7 — that is the other design, and Raft is the one being built here.
- **No security.** Plaintext JSON, no auth, loopback only.
- **crc32 is not a cryptographic hash.** It catches accidental corruption and divergence, which is all it is used for. A real system uses SHA-256 in the Merkle tree.
- **UTC timestamps.** The standard library has no local clock and a dependency for one was not worth it. All three nodes agree, which is what matters for reading causality.

---

## 11. Where to look in the code

| Question | File |
|---|---|
| What do nodes say to each other? | `src/proto.rs` |
| How does consensus actually work? | `src/raft/node.rs` |
| What is on disk, and how is a torn write survived? | `src/raft/log_store.rs` |
| How is the map built from the log? | `src/statemachine.rs` |
| How is a partition faked? | `src/faults.rs`, `src/transport.rs` |
| How do three processes get supervised? | `src/cli/supervise.rs` |
| What proves any of this works? | `tests/cluster.rs` |

Start with `proto.rs` — the message list tells you what the system can do — then `raft/node.rs`, top to bottom. It reads in the order Raft happens.
