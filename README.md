# quorum

A distributed key-value database built from scratch in Go — a **log-structured merge-tree** storage engine replicated by **Raft** consensus, designed to stay consistent and available as it scales across a cluster.

Built the way the real ones are (the lineage behind Bigtable, RocksDB, Cassandra, CockroachDB, etcd) and small enough to read end to end.

> **Status — Phase 1 complete: the single-node LSM storage engine.**
> The engine below is implemented and tested. Raft replication and sharding are next; see the roadmap.

---

## Why this exists

Two questions sit at the heart of every scalable stateful system:

1. **How do you store data on one node so writes are fast and nothing is lost on a crash?** → a log-structured merge-tree.
2. **How do you keep many nodes agreeing on that data as machines fail?** → a consensus protocol (Raft).

`quorum` implements both, from first principles, so the mechanics are visible rather than hidden behind a library.

---

## Phase 1 — the storage engine (done)

An LSM tree turns random writes into sequential ones: every write is appended to a log and buffered in memory, then flushed to immutable sorted files that are periodically merged. That is what makes write-heavy workloads cheap.

```
        write path                                 read path
        ──────────                                 ─────────
  Put/Delete                                   Get(key)
     │                                            │
     ▼                                            ▼
  ┌────────┐  append   ┌──────────────┐      memtable ──hit──► value
  │  WAL   │◄──────────│   memtable   │          │
  │ (crash │           │  (skiplist)  │        miss│
  │ safe)  │           └──────┬───────┘            ▼
  └────────┘                  │ full          SSTable N  (newest)
                              ▼ flush              │ bloom-filter gate
                        ┌──────────────┐        miss│
                        │  SSTable(s)  │◄───────────┘
                        │  immutable,  │        ...  ► SSTable 0 (oldest)
                        │  sorted, w/  │
                        │  bloom+index │   background: compaction merges SSTables,
                        └──────────────┘   dropping overwritten values & tombstones
```

### What's implemented

| Component | File | Notes |
| :-- | :-- | :-- |
| **Write-ahead log** | `internal/lsm/wal.go` | CRC32-framed records; a torn tail from a crash mid-write stops replay cleanly instead of corrupting recovery. |
| **Memtable** | `internal/lsm/memtable.go` | Concurrency-safe **skiplist** — O(log n) ordered writes, sorted iteration for flushing. |
| **SSTable** | `internal/lsm/sstable.go` | Immutable sorted file with a **sparse index** (one anchor per 16 entries) and a **Bloom filter** to skip tables that can't hold a key. |
| **Bloom filter** | `internal/lsm/bloom.go` | Double-hashing (Kirsch–Mitzenmacher); size/probe count derived from target false-positive rate. |
| **Engine** | `internal/lsm/db.go` | Ties it together: WAL rotation on flush, newest-first read path, tombstones, full compaction, crash recovery on open. |
| **CLI** | `cmd/quorum` | `put`/`get`/`del`/`flush` shell to drive the engine by hand. |

### Design notes worth knowing

- **Durability is a choice.** `SyncWrites` fsyncs the WAL on every write (safe) or batches it (fast); the tradeoff is explicit in `Options`.
- **Deletes are tombstones.** A delete writes a marker that shadows older on-disk values until compaction removes both.
- **Tombstone GC is only safe here because compaction is *full*** — every SSTable participates, so no older value can be un-shadowed. Leveled compaction (roadmap) must keep a tombstone until it reaches the bottom level. This subtlety is called out in `db.go`.

---

## Try it

```bash
go test ./...            # WAL replay, SSTable round-trip, recovery, compaction
go run ./cmd/quorum ./data
> put user:1 deepak
> get user:1
deepak
> del user:1
> get user:1
(not found)
```

Kill the process before `flush` and reopen — the data comes back from the WAL.

---

## Roadmap

- [x] **Phase 1 — LSM storage engine**: WAL, skiplist memtable, SSTables with bloom filters + sparse index, full compaction, crash recovery.
- [ ] **Phase 1.5 — engine polish**: block compression, an ordered range/scan iterator, leveled compaction with correct tombstone lifetime.
- [ ] **Phase 2 — Raft consensus**: leader election, log replication, snapshots; the replicated log drives the state machine (this engine) so a majority — a *quorum* — agrees on every write.
- [ ] **Phase 3 — distributed layer**: a networked KV service over Raft, linearizable reads, then **sharding** (range or hash partitions) with per-shard Raft groups to scale horizontally.

The name is the goal: a write is committed once a **quorum** of replicas has durably agreed on it.
