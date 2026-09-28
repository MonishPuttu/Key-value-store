# 3. Compaction

Without compaction an LSM tree would accumulate L0 files forever: reads would check hundreds of
files and every overwritten or deleted value would occupy disk forever. Compaction merges files to
bound **read amplification** and **space amplification**, paying for it with **write
amplification**. Code: [`engine/compaction.go`](../engine/compaction.go).

## 3.1 Leveled layout

```mermaid
flowchart TB
    L0["L0 — flushed memtables · files OVERLAP · trigger: 4 files"]
    L1["L1 — non-overlapping · target 10 MiB"]
    L2["L2 — non-overlapping · target 100 MiB"]
    L3["L3 — non-overlapping · target 1 GiB"]
    L4["L4 … L6 — ×10 each"]
    L0 --> L1 --> L2 --> L3 --> L4
```

- **L0** files come straight from memtables, so their key ranges overlap: a read may check all of
  them. Their count is therefore kept small.
- **L1+** are each one sorted run split into ~2 MiB files with **disjoint** key ranges: a read checks
  at most **one file per level**.
- Each level is ~10× bigger than the previous one, so ~90 % of data lives in the last level and
  space amplification stays ≈ 1.1×.

## 3.2 Picking what to compact

A background goroutine computes a **score** per level and compacts the highest score ≥ 1:

```
score(L0) = number of L0 files / L0CompactionTrigger (4)
score(Ln) = bytes in Ln / (BaseLevelSize × 10^(n-1))
```

```mermaid
flowchart TD
    S[scores] --> Q{max score ≥ 1?}
    Q -- no --> Idle[sleep until next flush]
    Q -- "L0" --> A["inputs = ALL L0 files<br/>+ overlapping L1 files"]
    Q -- "Ln" --> B["inputs = next file in Ln (round-robin cursor)<br/>+ overlapping Ln+1 files"]
    A & B --> T{"single Ln file and<br/>nothing overlaps in Ln+1?"}
    T -- yes --> MV["trivial move: re-assign the file<br/>(no I/O)"]
    T -- no --> MG["k-way merge → new files in Ln+1<br/>(split at TargetFileSize)"]
```

The **round-robin cursor** (`compPtr`) makes successive compactions of a level walk through the
whole key space instead of hammering one range.

A **trivial move** is a pure metadata change: e.g. sequential inserts produce L1 files that never
overlap L2, so they can be pushed down without rewriting a byte (`kvbench fillseq` shows many).

## 3.3 Merging

```mermaid
flowchart LR
    subgraph in [inputs]
      A["L1 file: a=1 · c=del · f=6"]
      B["L2 file: a=0 · b=2 · c=3 · g=7"]
    end
    in --> M["merging iterator<br/>newest wins per key"]
    M --> O["L2 output: a=1 · b=2 · c=tombstone* · f=6 · g=7"]
```

\* the tombstone is kept or dropped according to §3.4.

The same merging iterator used for scans produces the output; for each key only the newest version
survives, which is how overwritten data is reclaimed.

## 3.4 Tombstones: when can a delete be forgotten?

A tombstone must be kept as long as an **older** value for that key might exist in a **deeper**
level — dropping it early would *resurrect* the deleted value:

```mermaid
sequenceDiagram
    participant L1
    participant L2
    participant L3
    Note over L3: c = "old"
    Note over L1: c = tombstone
    L1->>L2: compact L1→L2, drop tombstone ❌
    Note over L2: (no entry for c)
    Note over L3: c = "old"  ← Get(c) now returns "old"!
```

So the engine drops tombstones only when **no level below the output overlaps the compaction's
key range** (`dropTombstones` in `runCompaction`). In the example above the tombstone travels down
with the data until it meets and cancels the old value.

## 3.5 Installing the result atomically

```mermaid
sequenceDiagram
    participant C as Compaction goroutine
    participant V as current version
    participant M as MANIFEST
    participant R as Readers
    C->>C: write new SSTables (fsync each)
    C->>V: lock · new version = current − inputs + outputs
    C->>M: write MANIFEST.tmp · fsync · rename · fsync dir
    C->>V: swap current, unref old version, mark inputs obsolete
    R-->>V: readers still holding old version keep reading old files
    Note over V: last reader releases → obsolete files deleted
```

The new version is derived from the **current** version at install time, not the one the
compaction started from, because flushes may have added L0 files in the meantime.

If the process crashes before the manifest rename, the new files are orphans and are deleted on
the next `Open`; the old files are still referenced, so nothing is lost.

## 3.6 Write stalls

Compaction is the engine's garbage collector; if writes outpace it, reads degrade and memory grows.
The engine applies backpressure:

| Condition | Effect |
| --- | --- |
| `MaxImmutableMemtables` (2) waiting for flush | writers wait |
| `L0StopWritesTrigger` (12) L0 files | writers wait |

`write_stalls` in `/v1/stats` counts these events. Frequent stalls mean the disk cannot sustain the
write rate: raise `MemtableSize`, add I/O capacity, or reduce write volume.

## 3.7 Write amplification, measured

Each byte is written: once to the WAL, once when flushed, and roughly once per level it moves
through (more when a level's file overlaps several files below). The engine reports

```
write_amplification = (user bytes + flush bytes + compaction bytes written) / user bytes
```

For the `kvbench` default run this is ≈ 4. Production LSMs typically see 10–30× — a key trade-off
you can explore in [Exercise 3](08-exercises.md).

## 3.8 Leveled vs tiered compaction

| | Leveled (this engine, LevelDB/RocksDB default) | Size-tiered (Cassandra STCS) |
| --- | --- | --- |
| Shape | one sorted run per level | several similarly-sized runs per tier |
| Read amp | low (1 file per level) | higher |
| Space amp | low (~1.1×) | high (up to 2× during compaction) |
| Write amp | higher | lower |

Next: [4. Durability & recovery →](04-durability-and-recovery.md)
