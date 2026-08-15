package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

// Persister stores the Raft state that MUST survive a crash: currentTerm,
// votedFor, and the log. Without it, a rebooted node could vote twice in one
// term or lose committed entries, breaking safety.
type Persister interface {
	Save(state []byte) error
	Load() ([]byte, error)
}

// persistentState is the exact set of fields Raft must durably remember.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
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

// MemoryPersister keeps state in RAM. Suitable for tests and for simulating a
// reboot: hand the same MemoryPersister to a fresh Raft node and it recovers.
type MemoryPersister struct {
	mu    sync.Mutex
	state []byte
}

func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

func (p *MemoryPersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	return nil
}

func (p *MemoryPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), nil
}

// FilePersister writes state to a single file, replaced atomically via a
// temp-file rename so a crash mid-write can never leave a half-written state.
type FilePersister struct {
	path string
}

func NewFilePersister(path string) *FilePersister { return &FilePersister{path: path} }

func (p *FilePersister) Save(state []byte) error {
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, state, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}

func (p *FilePersister) Load() ([]byte, error) {
	b, err := os.ReadFile(p.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}
