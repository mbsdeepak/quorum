package kv

import (
	"errors"
	"sync"
	"time"

	"github.com/mbsdeepak/quorum/internal/lsm"
	"github.com/mbsdeepak/quorum/internal/raft"
)

// ErrNotLeader is returned when a write is attempted on a non-leader node. The
// caller should retry against another node.
var ErrNotLeader = errors.New("kv: not leader")

// ErrTimeout is returned when a write is accepted by the leader but not committed
// within the deadline (e.g. leadership was lost before it replicated).
var ErrTimeout = errors.New("kv: commit timed out")

const applyTimeout = 3 * time.Second

// Store is one replica: a Raft node plus the local LSM engine it drives. Writes
// go through Raft; reads are served locally from the LSM store.
type Store struct {
	rf *raft.Raft
	db *lsm.DB

	mu          sync.Mutex
	lastApplied int
	waiters     map[int]chan struct{} // log index -> closed when applied here
}

// NewStore creates a replica. peers lists every node id (including id). db is the
// node's own LSM engine; each replica owns a separate one.
func NewStore(id int, peers []int, trans raft.Transport, persister raft.Persister, db *lsm.DB) *Store {
	ch := make(chan raft.ApplyMsg, 256)
	s := &Store{
		db:      db,
		waiters: make(map[int]chan struct{}),
	}
	s.rf = raft.Make(id, peers, trans, persister, ch)
	go s.applyLoop(ch)
	return s
}

// applyLoop applies committed commands to the local LSM store in log order and
// wakes any writer waiting on those indices.
func (s *Store) applyLoop(ch chan raft.ApplyMsg) {
	for m := range ch {
		if !m.CommandValid {
			continue
		}
		if cmd, ok := DecodeCommand(m.Command); ok {
			switch cmd.Op {
			case OpPut:
				_ = s.db.Put(cmd.Key, cmd.Value)
			case OpDelete:
				_ = s.db.Delete(cmd.Key)
			}
		}
		s.mu.Lock()
		s.lastApplied = m.CommandIndex
		for idx, w := range s.waiters {
			if idx <= s.lastApplied {
				close(w)
				delete(s.waiters, idx)
			}
		}
		s.mu.Unlock()
	}
}

// waitCh returns a channel closed once index has been applied locally.
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

// Put replicates key=val through Raft and returns once it is committed and
// applied on this node.
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

// Get reads key from the local LSM store. This is a local read: it reflects
// everything this node has applied, but is not linearizable across the cluster
// (a stale follower may lag the leader). Leader-lease / read-index reads are on
// the roadmap.
func (s *Store) Get(key []byte) ([]byte, error) { return s.db.Get(key) }

// IsLeader reports whether this replica currently believes it is the leader.
func (s *Store) IsLeader() bool {
	_, isLeader, _ := s.rf.Status()
	return isLeader
}

// AppliedIndex is the highest log index applied to the local store.
func (s *Store) AppliedIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

// Close stops the Raft node. The caller owns closing the LSM store.
func (s *Store) Close() { s.rf.Kill() }
