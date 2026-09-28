# 8. Exercises

## Lab 1 — Watch an LSM tree grow (20 min)

```bash
go run ./cmd/kvbench -n 1000000 -benchmarks fillrandom
go run ./cmd/kvbench -n 1000000 -benchmarks fillseq
```

Compare the level shapes, number of trivial moves and write amplification. Why does sequential
insertion produce so many trivial moves?

## Lab 2 — Look inside the files (30 min)

```bash
go run ./cmd/kvbench -dir /tmp/lsm -n 100000 -benchmarks fillrandom
ls -la /tmp/lsm && cat /tmp/lsm/MANIFEST | head -c 600
xxd /tmp/lsm/$(ls /tmp/lsm | grep sst | head -1) | tail -5    # the footer
```

Decode the footer by hand: index offset, bloom offset, magic `KVSSTBL1`.

## Lab 3 — Amplification trade-offs (45 min)

Write a small program using `engine.Open` with different options and a fixed random-write workload:

| Run | Change |
| --- | --- |
| A | defaults |
| B | `LevelMultiplier: 4` |
| C | `MemtableSize: 64 << 20` |
| D | `L0CompactionTrigger: 8` |

Record write amplification, level shape and `readrandom` throughput. Explain each difference using
[chapter 3](03-compaction.md).

## Lab 4 — Resurrection bug (30 min)

In `compaction.go`, change `dropTombstones` to always be `true`. Write a test that:
puts `k=old`, flushes and compacts it to a deep level, deletes `k`, forces compactions of the upper
levels, then `Get(k)`. Watch the deleted value come back. Restore the code.

## Lab 5 — Crash consistency (30 min)

Extend `TestRecoveryFromWALWithoutClose`: truncate the WAL copy at every byte offset from its end
back to its start, open the DB each time, and assert that the recovered state is always a
**prefix** of the batches written (never a partial batch).

## Lab 6 — Kill the leader (20 min)

```bash
make local-cluster
./bin/kvctl bench -n 200000 -c 16 &          # continuous load
./bin/kvctl status | grep leader_id
pkill -f "kvstore -id n1"                     # if n1 is leader
./bin/kvctl -addr http://127.0.0.1:8082 status
```

How long was the cluster unavailable for writes? Which requests failed and why? Restart n1 and
watch it catch up (`applied_index`).

## Lab 7 — Linearizability violation (1 h)

Start a 3-node cluster, partition the leader (e.g. `iptables` on its Raft port, or pause it with
`kill -STOP`), write through another node, then read from the old leader with
`consistency=leader` and with `consistency=strong`. Explain the results using §5.5.

## Lab 8 — Add TTLs (2 h)

Add `PUT /v1/kv/{key}?ttl=30s`. Hint: store an expiry timestamp with the value; the *leader*
decides expiry times and puts them in the log entry (never call `time.Now()` inside `FSM.Apply` —
why?). Filter expired keys on read, and remove them during compaction.

## Lab 9 — Prefix compression (2 h)

SSTable blocks store full keys. Implement LevelDB-style prefix compression (store the length of the
prefix shared with the previous key, plus the suffix) with restart points every 16 keys so binary
search within a block still works. Measure the size reduction on `users/000001…` keys.

## Review questions

1. Why must a tombstone be written instead of simply removing the key from the memtable?
2. Why are L0 files allowed to overlap, but not L1+ files?
3. What guarantees that an SSTable referenced by the manifest is complete?
4. Why does the FSM store `applied_index` in the same batch as the data?
5. A 4-node cluster tolerates how many failures? Why is that worse than it sounds?
6. Why is `VerifyLeader` necessary for strong reads even though the node "is" the leader?
7. What could go wrong if `FSM.Apply` read the wall clock?
