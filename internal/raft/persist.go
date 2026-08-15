package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

// Persister stores what MUST survive a crash: the Raft state (currentTerm,
// votedFor, log) and, once log compaction kicks in, the latest snapshot. State
// is saved often (every term/vote/log change), so it is separated from the
// snapshot, which is large and changes rarely — SaveState rewrites only the
// small state, SaveStateAndSnapshot rewrites both atomically.
type Persister interface {
	SaveState(state []byte) error
	SaveStateAndSnapshot(state, snapshot []byte) error
	LoadState() ([]byte, error)
	LoadSnapshot() ([]byte, error)
}

// persistentState is the exact set of fields Raft must durably remember. With
// snapshots, the log is a suffix and LastIncluded{Index,Term} anchor it.
type persistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
	BaseConfig        []int // cluster config as of the snapshot boundary
}

func encodeState(s persistentState) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeState(b []byte) (persistentState, error) {
	var s persistentState
	if len(b) == 0 {
		return s, nil
	}
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&s)
	return s, err
}

// MemoryPersister keeps state and snapshot in RAM. Suitable for tests and for
// simulating a reboot: hand the same MemoryPersister to a fresh node and it
// recovers.
type MemoryPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

func (p *MemoryPersister) SaveState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	return nil
}

func (p *MemoryPersister) SaveStateAndSnapshot(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	p.snapshot = append([]byte(nil), snapshot...)
	return nil
}

func (p *MemoryPersister) LoadState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), nil
}

func (p *MemoryPersister) LoadSnapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.snapshot...), nil
}

// FilePersister writes state and snapshot to separate files, each replaced
// atomically via a temp-file rename so a crash mid-write can never leave a
// half-written file.
type FilePersister struct {
	statePath string
	snapPath  string
}

func NewFilePersister(statePath, snapPath string) *FilePersister {
	return &FilePersister{statePath: statePath, snapPath: snapPath}
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (p *FilePersister) SaveState(state []byte) error { return writeAtomic(p.statePath, state) }

func (p *FilePersister) SaveStateAndSnapshot(state, snapshot []byte) error {
	if err := writeAtomic(p.snapPath, snapshot); err != nil {
		return err
	}
	return writeAtomic(p.statePath, state)
}

func readMaybe(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}

func (p *FilePersister) LoadState() ([]byte, error)    { return readMaybe(p.statePath) }
func (p *FilePersister) LoadSnapshot() ([]byte, error) { return readMaybe(p.snapPath) }
