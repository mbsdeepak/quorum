package kv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mbsdeepak/quorum/internal/lsm"
	"github.com/mbsdeepak/quorum/internal/raft"
)

// ErrNotLeader is returned when a write is attempted on a non-leader node.
var ErrNotLeader = errors.New("kv: not leader")

// ErrTimeout is returned when a write is accepted by the leader but not committed
// within the deadline.
var ErrTimeout = errors.New("kv: commit timed out")

const (
	applyTimeout = 3 * time.Second
	// snapshotEvery triggers a snapshot (and log compaction) every N applied
	// commands. Small here so snapshots exercise easily; production would tie
	// this to log byte size.
	snapshotEvery = 50
)

// Store is one replica: a Raft node plus the local LSM engine it drives. Writes
// go through Raft; reads are served locally from the LSM store. The Store owns
// its LSM directory so it can rebuild the engine when Raft installs a snapshot.
type Store struct {
	rf     *raft.Raft
	dir    string
	lsmDir string

	dbMu sync.RWMutex // guards the db pointer across snapshot rebuilds
	db   *lsm.DB

	mu          sync.Mutex
	lastApplied int
	waiters     map[int]chan struct{}
}

// NewStore creates a replica rooted at dir. The LSM engine lives in dir/lsm; the
// Raft persister (passed in) should live elsewhere so a snapshot rebuild, which
// wipes dir/lsm, never touches Raft's own state.
func NewStore(id int, peers []int, trans raft.Transport, persister raft.Persister, dir string) (*Store, error) {
	lsmDir := filepath.Join(dir, "lsm")
	db, err := lsm.Open(lsmDir, lsm.DefaultOptions())
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:     dir,
		lsmDir:  lsmDir,
		db:      db,
		waiters: make(map[int]chan struct{}),
	}
	ch := make(chan raft.ApplyMsg, 256)
	s.rf = raft.Make(id, peers, trans, persister, ch)
	go s.applyLoop(ch)
	return s, nil
}

// applyLoop applies committed commands and installed snapshots to the local LSM
// store, and periodically snapshots to let Raft compact its log.
func (s *Store) applyLoop(ch chan raft.ApplyMsg) {
	for m := range ch {
		switch {
		case m.SnapshotValid:
			s.installSnapshot(m.Snapshot)
			s.markApplied(m.SnapshotIndex)

		case m.CommandValid:
			if cmd, ok := DecodeCommand(m.Command); ok {
				s.dbMu.RLock()
				switch cmd.Op {
				case OpPut:
					_ = s.db.Put(cmd.Key, cmd.Value)
				case OpDelete:
					_ = s.db.Delete(cmd.Key)
				}
				s.dbMu.RUnlock()
			}
			s.markApplied(m.CommandIndex)
			if m.CommandIndex%snapshotEvery == 0 {
				s.takeSnapshot(m.CommandIndex)
			}
		}
	}
}

// markApplied records progress and wakes any writers waiting on committed indices.
func (s *Store) markApplied(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index > s.lastApplied {
		s.lastApplied = index
	}
	for idx, w := range s.waiters {
		if idx <= s.lastApplied {
			close(w)
			delete(s.waiters, idx)
		}
	}
}

// takeSnapshot serializes the full LSM state and hands it to Raft for compaction.
func (s *Store) takeSnapshot(index int) {
	s.dbMu.RLock()
	items, err := s.db.Items()
	s.dbMu.RUnlock()
	if err != nil {
		return
	}
	s.rf.Snapshot(index, encodeItems(items))
}

// installSnapshot rebuilds the LSM store from a snapshot: the engine is closed,
// its directory wiped, and the snapshot's key/value pairs reloaded. This is how a
// follower that fell behind the leader's compacted log gets a fresh, correct
// state (including keys deleted before the snapshot, which are simply absent).
func (s *Store) installSnapshot(data []byte) {
	items := decodeItems(data)
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	s.db.Close()
	os.RemoveAll(s.lsmDir)
	db, err := lsm.Open(s.lsmDir, lsm.DefaultOptions())
	if err != nil {
		return
	}
	for _, kv := range items {
		_ = db.Put(kv.Key, kv.Value)
	}
	s.db = db
}

func (s *Store) waitCh(index int) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan struct{})
	if s.lastApplied >= index {
		close(ch)
		return ch
	}
	s.waiters[index] = ch
	return ch
}

// Put replicates key=val through Raft and returns once committed and applied here.
func (s *Store) Put(key, val []byte) error { return s.propose(Command{Op: OpPut, Key: key, Value: val}) }

// Delete replicates a deletion of key through Raft.
func (s *Store) Delete(key []byte) error { return s.propose(Command{Op: OpDelete, Key: key}) }

func (s *Store) propose(cmd Command) error {
	index, _, isLeader := s.rf.Start(cmd.Encode())
	if !isLeader {
		return ErrNotLeader
	}
	select {
	case <-s.waitCh(index):
		return nil
	case <-time.After(applyTimeout):
		return ErrTimeout
	}
}

// Get reads key from the local LSM store (a local, not linearizable, read).
func (s *Store) Get(key []byte) ([]byte, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Get(key)
}

// IsLeader reports whether this replica currently believes it is the leader.
func (s *Store) IsLeader() bool {
	_, isLeader, _ := s.rf.Status()
	return isLeader
}

// Raft exposes the underlying consensus node so a transport layer can serve its
// RPC handlers (e.g. the TCP server in package netrpc).
func (s *Store) Raft() *raft.Raft { return s.rf }

// AppliedIndex is the highest log index applied to the local store.
func (s *Store) AppliedIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

// RaftLogLength is the number of live (uncompacted) Raft log entries.
func (s *Store) RaftLogLength() int { return s.rf.LogLength() }

// SnapshotIndex is the last log index folded into a snapshot (0 = none).
func (s *Store) SnapshotIndex() int { return s.rf.SnapshotIndex() }

// Close stops the Raft node and closes the LSM engine.
func (s *Store) Close() {
	s.rf.Kill()
	s.dbMu.Lock()
	s.db.Close()
	s.dbMu.Unlock()
}

// ---- snapshot codec (full key/value state) --------------------------------

func encodeItems(items []lsm.KV) []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(items)
	return buf.Bytes()
}

func decodeItems(b []byte) []lsm.KV {
	var items []lsm.KV
	if len(b) > 0 {
		_ = gob.NewDecoder(bytes.NewReader(b)).Decode(&items)
	}
	return items
}
