# Changelog

## [0.3.0] — 2026-08-16

quorum is a distributed key-value database built from scratch in Go.

- **LSM storage engine** — WAL, skiplist memtable, SSTables (bloom filters + sparse index), compaction, crash recovery.
- **Raft consensus** — leader election, log replication, crash-safe persistence; a 3-node cluster replicates to independent engines and survives leader failure.
- **Snapshots** — log compaction via `Snapshot` / `InstallSnapshot`, so the log stays bounded and a far-behind follower catches up.

Try it: `go run ./cmd/quorum-demo`

_Next: dynamic membership changes, then a networked transport and sharding._
