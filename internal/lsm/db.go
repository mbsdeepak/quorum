// Package lsm is a single-node, log-structured merge-tree key-value store.
//
// Writes go to a write-ahead log (durability) and an in-memory memtable
// (recent data). When the memtable fills, it is flushed to an immutable,
// sorted, on-disk SSTable and the WAL is rotated. Reads consult the memtable
// first, then SSTables newest-to-oldest, so newer writes shadow older ones.
// Periodic compaction merges SSTables, discarding overwritten values and
// tombstones to reclaim space.
//
// This engine is the durable storage layer that the Raft package (Phase 2)
// replicates across a cluster to build a distributed database.
package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Public and internal sentinel errors.
var (
	ErrNotFound    = errors.New("lsm: key not found")
	errShortRecord = errors.New("lsm: short record")
	errCorruptSST  = errors.New("lsm: corrupt sstable")
)

// Options tune the engine.
type Options struct {
	// MemtableThreshold is the approximate live-byte size at which the active
	// memtable is flushed to a new SSTable.
	MemtableThreshold int64
	// CompactionTrigger is the SSTable count that kicks off a compaction.
	CompactionTrigger int
	// SyncWrites fsyncs the WAL on every write when true: durable but slower.
	// When false, writes are only guaranteed durable after an explicit flush
	// or clean close.
	SyncWrites bool
}

// DefaultOptions returns sensible defaults for local use.
func DefaultOptions() Options {
	return Options{
		MemtableThreshold: 4 << 20, // 4 MiB
		CompactionTrigger: 4,
		SyncWrites:        true,
	}
}

// DB is the key-value store. It is safe for concurrent use.
type DB struct {
	dir     string
	opts    Options
	walPath string

	mu     sync.RWMutex
	mem    *memtable
	wal    *WAL
	ssts   []*sstReader // newest-first: ssts[0] shadows later tables
	sstSeq uint64       // monotonic SSTable id
}

// Open opens (or creates) a database rooted at dir.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		dir:     dir,
		opts:    opts,
		walPath: filepath.Join(dir, "wal.log"),
		mem:     newMemtable(),
	}
	// Load any SSTables left by a previous run (newest seq first).
	if err := db.loadSSTables(); err != nil {
		return nil, err
	}
	// Rebuild the memtable from WAL records written since the last flush.
	if err := ReplayWAL(db.walPath, func(e walEntry) error {
		db.mem.put(e.kind, e.key, e.val)
		return nil
	}); err != nil {
		return nil, err
	}
	wal, err := OpenWAL(db.walPath)
	if err != nil {
		return nil, err
	}
	db.wal = wal
	return db, nil
}

func (db *DB) sstPath(seq uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("sst-%020d.dat", seq))
}

func (db *DB) loadSSTables() error {
	matches, err := filepath.Glob(filepath.Join(db.dir, "sst-*.dat"))
	if err != nil {
		return err
	}
	type loaded struct {
		seq uint64
		r   *sstReader
	}
	var ls []loaded
	for _, p := range matches {
		base := filepath.Base(p)
		numStr := strings.TrimSuffix(strings.TrimPrefix(base, "sst-"), ".dat")
		seq, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue
		}
		r, err := openSSTable(p)
		if err != nil {
			return err
		}
		ls = append(ls, loaded{seq: seq, r: r})
		if seq > db.sstSeq {
			db.sstSeq = seq
		}
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].seq > ls[j].seq }) // newest first
	db.ssts = db.ssts[:0]
	for _, l := range ls {
		db.ssts = append(db.ssts, l.r)
	}
	return nil
}

// Put stores val under key.
func (db *DB) Put(key, val []byte) error { return db.write(kindPut, key, val) }

// Delete removes key, writing a tombstone that shadows older on-disk values.
func (db *DB) Delete(key []byte) error { return db.write(kindDelete, key, nil) }

func (db *DB) write(kind recordKind, key, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Append(kind, key, val); err != nil {
		return err
	}
	if db.opts.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}
	db.mem.put(kind, key, val)

	if db.mem.approxSize() >= db.opts.MemtableThreshold {
		return db.flushLocked()
	}
	return nil
}

// Get returns the value for key, or ErrNotFound if it is absent or deleted.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if v, kind, ok := db.mem.get(key); ok {
		if kind == kindDelete {
			return nil, ErrNotFound
		}
		return append([]byte(nil), v...), nil
	}
	for _, r := range db.ssts { // newest first
		v, kind, ok, err := r.get(key)
		if err != nil {
			return nil, err
		}
		if ok {
			if kind == kindDelete {
				return nil, ErrNotFound
			}
			return v, nil
		}
	}
	return nil, ErrNotFound
}

// Flush forces the active memtable to disk. Useful for clean shutdown and tests.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

// flushLocked writes the memtable to a new SSTable, rotates the WAL, and installs
// the table at the front of the stack. The caller must hold db.mu.
func (db *DB) flushLocked() error {
	entries := db.mem.scan()
	if len(entries) == 0 {
		return nil
	}
	db.sstSeq++
	path := db.sstPath(db.sstSeq)
	if err := writeSSTable(path, entries); err != nil {
		return err
	}
	r, err := openSSTable(path)
	if err != nil {
		return err
	}
	db.ssts = append([]*sstReader{r}, db.ssts...) // newest first

	// The flushed data is now durable in the SSTable, so the WAL can be reset.
	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(db.walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	wal, err := OpenWAL(db.walPath)
	if err != nil {
		return err
	}
	db.wal = wal
	db.mem = newMemtable()

	if len(db.ssts) >= db.opts.CompactionTrigger {
		return db.compactLocked()
	}
	return nil
}

// compactLocked merges all SSTables into one, keeping the newest value per key
// and dropping tombstones. Dropping tombstones is safe ONLY because this is a
// full compaction — no older table survives to be un-shadowed. (Leveled
// compaction, on the roadmap, must keep a tombstone until it reaches the
// bottom level.) The caller must hold db.mu.
func (db *DB) compactLocked() error {
	seen := make(map[string]struct{})
	var merged []entry
	for _, r := range db.ssts { // newest first: first sighting of a key wins
		es, err := r.all()
		if err != nil {
			return err
		}
		for _, e := range es {
			if _, ok := seen[string(e.key)]; ok {
				continue
			}
			seen[string(e.key)] = struct{}{}
			if e.kind == kindDelete {
				continue
			}
			merged = append(merged, e)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return bytes.Compare(merged[i].key, merged[j].key) < 0 })

	db.sstSeq++
	path := db.sstPath(db.sstSeq)
	if err := writeSSTable(path, merged); err != nil {
		return err
	}
	newReader, err := openSSTable(path)
	if err != nil {
		return err
	}

	old := db.ssts
	db.ssts = []*sstReader{newReader}
	for _, r := range old {
		p := r.f.Name()
		r.close()
		os.Remove(p)
	}
	return nil
}

// Close flushes buffered WAL data and releases all file handles. It does NOT
// flush the memtable — those writes remain recoverable from the WAL on reopen.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	var firstErr error
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			firstErr = err
		}
	}
	for _, r := range db.ssts {
		if err := r.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
