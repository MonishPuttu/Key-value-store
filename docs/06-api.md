# 6. API & CLI Reference

## 6.1 HTTP API

Any node accepts any request; writes and strong reads are forwarded to the leader.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/kv/{key}?consistency=strong\|leader\|stale` | Read a value (raw bytes). `404` if absent |
| `PUT` | `/v1/kv/{key}` | Write the request body as the value (≤ 1 MiB). `204` |
| `DELETE` | `/v1/kv/{key}` | Delete. `204` (idempotent) |
| `POST` | `/v1/cas/{key}` | Compare-and-swap |
| `POST` | `/v1/batch` | Atomic multi-key write |
| `GET` | `/v1/scan?prefix=&start=&end=&limit=&consistency=` | Range scan |
| `GET` | `/v1/stats` | Engine metrics of this node |
| `GET` | `/cluster/status` | Raft state, leader, members |
| `POST` | `/cluster/join` | Add a node |
| `DELETE` | `/cluster/nodes/{id}` | Remove a node |
| `POST` | `/cluster/snapshot` | Force a Raft snapshot |
| `GET` | `/healthz` · `/readyz` · `/metrics` | Liveness · has a leader · Prometheus |

Keys may contain `/` (`/v1/kv/users/42/profile`); percent-encode other special characters.

### Compare-and-swap

```http
POST /v1/cas/lock
{"expected_absent": true, "value": "owner-1"}      # create only if missing
{"expected": "owner-1", "value": "owner-2"}         # swap only if current == expected
```

`200 {"swapped": true}` or `409 {"swapped": false, "exists": true, "current": "owner-1"}`.

### Batch

```http
POST /v1/batch
{"ops": [
  {"op": "set", "key": "account/a", "value": "90"},
  {"op": "set", "key": "account/b", "value": "110"},
  {"op": "delete", "key": "pending/tx-17"}
]}
```

All operations are in one Raft entry and one engine batch: all or nothing, on every replica.

### Scan

```bash
curl 'localhost:8081/v1/scan?prefix=users/&limit=2'
{"items":[{"key":"users/1","value":"x"},{"key":"users/2","value":"y"}],"next":"users/42"}
```

Continue with `start=<next>`. `end` is exclusive.

### Status

```json
{"node_id":"n2","state":"Follower","leader_id":"n1","term":"2","commit_index":7,"applied_index":7,
 "nodes":[{"id":"n1","raft_addr":"127.0.0.1:7001","http_addr":"http://127.0.0.1:8081","suffrage":"Voter","leader":true}, …]}
```

### Errors

| Code | Meaning |
| --- | --- |
| 400 | Bad key/body |
| 404 | Key not found |
| 409 | CAS precondition failed |
| 413 | Value too large |
| 502 | Forwarding to the leader failed |
| 503 | No leader (election in progress or quorum lost) — retry with backoff |

## 6.2 `kvctl`

```
kvctl [-addr URL] get <key> [-c strong|leader|stale]
kvctl put <key> <value|->          # "-" reads stdin
kvctl del <key>
kvctl cas <key> <expected|-> <new> # "-" = must not exist
kvctl scan [-prefix p] [-start s] [-end e] [-limit n]
kvctl status | stats | snapshot
kvctl remove <node-id>
kvctl bench [-n 20000] [-c 32] [-size 100] [-read-ratio 0.8] [-keys 10000]
```

`KV_ADDR` sets the default address.

```
$ kvctl bench -n 20000 -c 32 -keys 5000
reads    16000 ops      8538 ops/s  p50=1.67ms    p99=7.91ms
writes    4000 ops      2135 ops/s  p50=6.11ms    p99=11.78ms
```

(3 nodes on one laptop; each write is replicated to a majority and fsynced.)

## 6.3 `kvstore` flags

| Flag | Env | Default | |
| --- | --- | --- | --- |
| `-id` | `KV_NODE_ID` | `n1` | Unique, stable node id |
| `-data` | `KV_DATA_DIR` | `./data/n1` | Engine, Raft log and snapshots |
| `-http` | `KV_HTTP_ADDR` | `:8080` | API listen address |
| `-http-advertise` | `KV_HTTP_ADVERTISE` | derived | URL peers use for forwarding |
| `-raft` | `KV_RAFT_ADDR` | `127.0.0.1:7000` | Raft bind address |
| `-raft-advertise` | `KV_RAFT_ADVERTISE` | bind addr | Required when binding `0.0.0.0` |
| `-bootstrap` | `KV_BOOTSTRAP=true` | false | Form a new cluster if no state exists |
| `-join` | `KV_JOIN` | — | HTTP URL of any member to join |
| `-memtable-mb` | — | 16 | Memtable size |
| `-cache-mb` | — | 64 | Block cache size |
| `-log-level` | `KV_LOG_LEVEL` | info | |

## 6.4 Embedded engine API (Go)

```go
db, err := engine.Open(dir, engine.Options{Sync: true, MemtableSize: 64 << 20})
defer db.Close()

db.Put(k, v)
v, err := db.Get(k)          // engine.ErrNotFound
db.Delete(k)

var b engine.Batch            // atomic
b.Put(k1, v1); b.Delete(k2)
db.Write(&b)

it, _ := db.NewIterator(start, end)   // [start, end), nil = unbounded
for ; it.Valid(); it.Next() { use(it.Key(), it.Value()) }
it.Close()

snap, _ := db.Snapshot()     // consistent point-in-time view
defer snap.Release()

db.Flush()                   // force memtable to disk
m := db.Metrics()            // levels, flushes, compactions, write amp, cache & bloom stats
```

Next: [7. Operations →](07-operations.md)
