# 5. Replication with Raft

One engine on one machine is a single point of failure. This chapter explains how
[`internal/cluster`](../internal/cluster) turns several engines into one fault-tolerant database.

## 5.1 The replicated state machine idea

> If every replica starts in the same state and applies **the same operations in the same order**,
> every replica ends in the same state.

So the problem reduces to agreeing on **one ordered log of operations**. That is what a consensus
algorithm provides; Raft is the most widely used one (etcd, Consul, CockroachDB, TiKV, …).

```mermaid
flowchart LR
    C[client: SET x=1] --> L
    subgraph L [Leader]
      LL["log: … | 41 SET y | 42 SET x=1"] --> LF[FSM → engine]
    end
    subgraph F1 [Follower]
      FL1["log: … | 41 | 42"] --> FF1[FSM → engine]
    end
    subgraph F2 [Follower]
      FL2["log: … | 41 | 42"] --> FF2[FSM → engine]
    end
    LL -- AppendEntries --> FL1 & FL2
```

## 5.2 Raft in five minutes

**Roles.** Each node is a *follower*, *candidate* or *leader*. Time is divided into numbered
**terms**; each term has at most one leader.

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: election timeout<br/>(no heartbeat from leader)
    Candidate --> Leader: votes from majority
    Candidate --> Follower: sees leader / higher term
    Candidate --> Candidate: split vote → new term
    Leader --> Follower: sees higher term
```

**Leader election.** A follower that hears nothing from a leader for a randomised election timeout
becomes a candidate, increments the term and asks for votes. A node votes at most once per term,
and only for a candidate whose log is at least as up to date as its own — which guarantees the new
leader has every committed entry. Randomised timeouts make split votes rare.

**Log replication.** The leader appends each command to its log and sends `AppendEntries` to
followers (also used as heartbeats). An entry is **committed** once stored on a **majority**. The
leader then applies it to its state machine and replies to the client; followers learn the commit
index from later messages and apply too.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant L as Leader (n1)
    participant F2 as Follower n2
    participant F3 as Follower n3
    C->>L: PUT /v1/kv/x = 1
    L->>L: append entry 42 (term 3) to log (fsync)
    par replicate
      L->>F2: AppendEntries(42)
      L->>F3: AppendEntries(42)
    end
    F2-->>L: ok (stored, fsync)
    Note over L: 2 of 3 have it → COMMITTED
    L->>L: FSM.Apply(42) → engine.Write
    L-->>C: 204 No Content
    F3-->>L: ok (late — doesn't matter)
    L->>F2: AppendEntries(commit=42) / heartbeat
    F2->>F2: FSM.Apply(42)
```

**Safety.** A committed entry is never lost or reordered: any future leader must have received
votes from a majority, and every majority overlaps the majority that stored the entry.

| Cluster size | Majority | Failures tolerated |
| --- | --- | --- |
| 1 | 1 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |

Even sizes add no tolerance (4 nodes tolerate 1, like 3) — use odd numbers.

This project uses **hashicorp/raft** for the protocol (elections, replication, log storage in
bbolt, snapshot transfer) and implements the parts specific to a database: the FSM, snapshots,
consistency levels, CAS, membership and request routing.

## 5.3 Commands and the FSM

Every mutation is a JSON `Command` in a log entry ([`fsm.go`](../internal/cluster/fsm.go)):

```json
{"op":"set","key":"eA==","value":"MQ=="}
{"op":"cas","key":"…","expected":"…","value":"…"}
{"op":"batch","ops":[{"op":"set",…},{"op":"delete",…}]}
{"op":"meta","key":"node/n2","value":"http://10.0.0.2:8080"}
```

Keys are namespaced inside the engine: user data under `u/`, cluster metadata under `m/`
(node HTTP addresses, the applied index).

The FSM implements `raft.BatchingFSM`: Raft hands it up to 64 committed entries at a time, which
it writes as **one engine batch with one fsync**, making durable replication fast.

## 5.4 Compare-and-swap is linearizable for free

CAS checks the current value **when the entry is applied**, in log order, on every replica:

```mermaid
sequenceDiagram
    participant A as Client A
    participant B as Client B
    participant L as Leader
    A->>L: CAS lock: absent → "A"
    B->>L: CAS lock: absent → "B"
    Note over L: log 50: CAS(absent→A)<br/>log 51: CAS(absent→B)
    L->>L: apply 50: lock absent ✓ → "A"
    L->>L: apply 51: lock = "A" ✗
    L-->>A: 200 swapped
    L-->>B: 409 current="A"
```

Both requests raced, but the log imposed an order, and every replica reaches the same verdict.
This is the primitive for distributed locks, leader leases and optimistic concurrency.

## 5.5 Reads and consistency levels

Writes always go through the log. Reads can choose ([`store.go`](../internal/cluster/store.go)):

| Level | How | Guarantee | Cost |
| --- | --- | --- | --- |
| `strong` (default) | **ReadIndex**: leader notes `commitIndex`, confirms leadership with a heartbeat round to a majority, waits until it has applied up to that index, then reads locally | Linearizable | 1 network round trip, no disk write |
| `leader` | Leader reads locally | Usually fresh; a deposed leader that has not noticed may be stale briefly | Local |
| `stale` | Any node reads locally | May lag the leader | Local, scales with replicas |

Why is `leader` not enough for linearizability? During a network partition an old leader may still
believe it leads while a new leader (in the majority partition) accepts writes:

```mermaid
sequenceDiagram
    participant C as Client
    participant Old as Old leader (minority side)
    participant New as New leader (majority side)
    Note over Old,New: network partition
    C->>New: PUT x=2 ✅ committed
    C->>Old: GET x (leader read)
    Old-->>C: x=1 ❌ stale
    C->>Old: GET x (strong read)
    Old->>Old: VerifyLeader → cannot reach majority
    Old-->>C: error / forwarded ✅
```

## 5.6 Exactly-once apply across restarts

Raft may re-deliver entries after a restart. The FSM stores `m/applied_index` **in the same engine
batch** as the entries' effects, so the engine and the index can never disagree. On restart:

```mermaid
flowchart LR
    A["engine: applied_index = 9008"] --> B["Raft replays entries > last snapshot"]
    B --> C{"entry.index ≤ 9008?"}
    C -- yes --> S[skip — already applied]
    C -- no --> AP[apply]
```

Because the engine is durable, the node is configured with `NoSnapshotRestoreOnStart`: it does
**not** rebuild its state from a snapshot at every boot, so restarts are fast regardless of data
size (`TestRestartDoesNotReapply`).

## 5.7 Snapshots and log compaction

The Raft log cannot grow forever. Every `SnapshotThreshold` entries (8192) Raft asks the FSM for a
snapshot and then truncates the log.

- `FSM.Snapshot()` calls `engine.Snapshot()` — a cheap point-in-time view (memtable rotation +
  pinned version). Raft guarantees no `Apply` runs concurrently with this call.
- `Persist()` streams every key/value as length-prefixed frames while new writes continue.
- A follower that is so far behind that the leader no longer has the entries it needs receives the
  snapshot (`InstallSnapshot`) and `FSM.Restore()` rebuilds its engine from the stream
  (`TestNewNodeCatchesUpFromSnapshot`).

```mermaid
sequenceDiagram
    participant L as Leader
    participant N as New / lagging node
    L->>N: AppendEntries(prev=120)
    N-->>L: reject: I only have up to 3
    Note over L: entries 4..119 were truncated
    L->>N: InstallSnapshot (stream)
    N->>N: Restore: wipe engine, bulk-load
    L->>N: AppendEntries(121…)
```

## 5.8 Membership and routing

- **Bootstrap**: the first node starts with `-bootstrap` and forms a one-node cluster.
- **Join**: a new node calls `POST /cluster/join` on any member; the leader calls `AddVoter`, and
  records the node's HTTP address in replicated metadata (`m/node/<id>`) so every node can find
  every other node's API.
- **Remove**: `DELETE /cluster/nodes/{id}`.
- **Routing**: any node accepts any request. A follower that receives a write or a `strong`/`leader`
  read proxies it to the leader's HTTP address (with a loop guard header).

```mermaid
flowchart LR
    C[client] --> F["follower n2"]
    F -- "ErrNotLeader → look up m/node/n1<br/>reverse proxy" --> L["leader n1"]
    L --> F --> C
```

## 5.9 Failure scenarios

| Failure | Behaviour |
| --- | --- |
| Follower dies | Cluster continues (majority intact); follower catches up from the log (or a snapshot) when it returns |
| Leader dies | Followers time out (~1 s), elect a new leader; in-flight writes that were not committed fail and should be retried by the client |
| Network partition | Majority side elects/keeps a leader and serves writes; minority side cannot commit or serve strong reads |
| Majority lost | No writes, no strong reads (safety over availability — CP in CAP terms); `stale` reads still work |
| Disk failure on a node | Replace the node: remove it, wipe, re-join; it receives a snapshot |

Next: [6. API & CLI →](06-api.md)
