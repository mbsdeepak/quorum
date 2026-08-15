# quorum

A distributed key-value database built from scratch in Go — a **log-structured merge-tree** storage engine replicated by **Raft** consensus, designed to stay consistent and available as it scales across a cluster.

Built the way the real ones are (the lineage behind Bigtable, RocksDB, Cassandra, CockroachDB, etcd) and small enough to read end to end.

> **Status — Phase 2.5: Raft with log compaction (snapshots).**
> The LSM engine (Phase 1), Raft consensus (Phase 2), and now **snapshot-based
> log compaction** are implemented and tested (race-clean). A 3-node cluster
> replicates writes to three independent LSM stores that converge, survives leader
> failure, compacts its log via snapshots, and catches up a far-behind follower
> with `InstallSnapshot`. Dynamic membership changes, a networked transport, and
> sharding are next; see the roadmap.
>
> Run the walkthrough: `go run ./cmd/quorum-demo`

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

## Phase 2 — Raft consensus (in progress)

A single node that loses its disk loses your data, and a single node can't scale reads or survive a machine failure. Raft fixes both: a cluster elects a **leader**, the leader replicates every write to **followers**, and a write is *committed* only once a **majority — a quorum — has stored it**. Committed entries are applied, in the same order on every node, to the LSM engine from Phase 1. The result is one logical database backed by N replicas that agree.

```
            client write
                 │
                 ▼
          ┌─────────────┐   AppendEntries    ┌─────────────┐
          │   LEADER    │ ─────────────────► │  follower   │
          │  (node 0)   │ ◄───── ack ─────── │  (node 1)   │
          │             │                    └──────┬──────┘
          │  raft log   │   AppendEntries    ┌──────┴──────┐
          │      │      │ ─────────────────► │  follower   │
          └──────┼──────┘ ◄───── ack ─────── │  (node 2)   │
                 │  committed once a          └──────┬──────┘
                 ▼  majority has stored it           ▼
           apply to LSM                        apply to LSM
        (each node's own engine — all three converge)
```

### What's implemented

| Component | File | Notes |
| :-- | :-- | :-- |
| **Consensus core** | `internal/raft/raft.go` | Leader election with randomized timeouts, log replication, majority-commit, and the ordered apply loop — following Raft paper Figure 2. |
| **RPC + wire types** | `internal/raft/types.go` | `RequestVote` / `AppendEntries` args & replies, including fast-backup conflict hints for O(terms) catch-up. |
| **Persistence** | `internal/raft/persist.go` | Durable `currentTerm` / `votedFor` / log; in-memory (tests) and atomic file-rename (crash-safe) persisters. |
| **Transport** | `internal/raft/transport.go` | Narrow send interface + an in-memory network that can drop traffic to simulate crashes and partitions. |
| **Replicated KV** | `internal/kv/` | Encodes writes as log commands and applies committed entries to each node's LSM store; local reads, leader-routed writes. |

### Correctness

Tested race-clean (`go test -race ./...`):

- **Election** — exactly one leader; re-election after a leader fails; **no leader without a quorum** (majority down ⇒ no writes, as it must be).
- **Replication** — all nodes agree on every committed index; a follower that was offline **catches up** when it returns.
- **Durability** — a rebooted node recovers its term, vote, and log from the persister.
- **End to end** — a 3-node cluster replicates puts/deletes to three separate LSM engines that converge, and committed writes **survive leader failure**.

A design note worth knowing: the leader only commits an entry from its **current term** by counting replicas (Raft §5.4.2) — committing a prior-term entry by vote count alone is unsafe and is explicitly avoided in `advanceCommitLocked`.

---

## Phase 2.5 — snapshots & log compaction (done)

Left alone, the Raft log grows forever. Snapshots fix that: the application periodically serializes its state up to some index and hands it to Raft, which then **discards the covered log prefix**. And when a follower has fallen so far behind that the leader has already compacted the entries it needs, the leader ships the whole **snapshot** instead of replaying the log.

```
before compaction                     after Snapshot(100)
  log: [1][2][3]...[100][101][102]      snapshot ≡ state@100
        └──── 102 entries ────┘         log: (100)[101][102]     ← 2 entries kept
                                              ▲ sentinel = boundary
```

- **`Snapshot(index, data)`** — application-driven compaction. The index math is rebased so absolute index → `i - lastIncludedIndex` (all routed through `*Locked` helpers to keep it in one place).
- **`InstallSnapshot` RPC** — a leader catches up a follower whose needed prefix is gone. The follower's applier delivers the snapshot to the state machine, which **rebuilds** from it (the kv `Store` wipes and reloads its LSM engine).
- **Boot from snapshot** — on restart a node restores `lastIncluded{Index,Term}` + snapshot, replays only the log tail, and re-delivers the snapshot so a volatile state machine can rebuild.
- **LSM `Items()`** — full-state enumeration (sorted, newest-wins, tombstones dropped) is what serializes the state machine for a snapshot.

Tested race-clean: the log provably shrinks under load, a partitioned follower is caught up via `InstallSnapshot`, and a rebooted node rebuilds from snapshot + tail. The demo's final section shows ~125 writes leaving only ~25 live log entries per node (`snapshot @ 100`).

A subtlety handled in the applier: the boot-from-snapshot delivery guard is `>=` (not `>`), because a freshly booted node's `lastApplied` already equals the snapshot index — a strict `>` would silently drop the snapshot and leave a volatile state machine empty.

---

## Try it

```bash
go test -race ./...      # LSM engine + Raft election/replication + replicated KV
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
- [x] **Phase 2 — Raft consensus**: leader election, log replication with fast-backup, crash-safe persistence, and the replicated log driving each node's LSM engine so a majority — a *quorum* — agrees on every write.
- [x] **Phase 2.5a — snapshots**: log compaction via `Snapshot` + `InstallSnapshot`, boot-from-snapshot recovery, and LSM full-state enumeration (`Items`).
- [ ] **Phase 2.5b — membership changes**: add/remove nodes at runtime via single-server config entries.
- [ ] **Phase 3 — distributed layer**: a real **networked transport** (replace the in-memory one), **linearizable reads** (read-index / leader lease), then **sharding** (range or hash partitions) with per-shard Raft groups to scale horizontally.
- [ ] **Engine polish**: block compression, an ordered range/scan iterator, leveled compaction with correct tombstone lifetime.

The name is the goal: a write is committed once a **quorum** of replicas has durably agreed on it.
