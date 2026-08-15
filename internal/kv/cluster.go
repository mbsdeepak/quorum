package kv

import (
	"fmt"
	"time"

	"github.com/mbsdeepak/quorum/internal/raft"
)

// Cluster is an in-process group of replicas wired over the in-memory network —
// a self-contained way to run and exercise quorum without real sockets. (Phase 3
// swaps the in-memory transport for a real networked one; this bootstrap stays.)
type Cluster struct {
	Net    *raft.InMemNetwork
	Stores []*Store
	down   []bool // nodes we've crashed/partitioned; a partitioned leader still
	// self-reports as leader until it can reach peers, so leader queries skip them
}

// NewInMemCluster starts one replica per directory in dirs, each with its own
// LSM engine, all connected through a shared in-memory network.
func NewInMemCluster(dirs []string) (*Cluster, error) {
	n := len(dirs)
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	c := &Cluster{Net: raft.NewInMemNetwork(), down: make([]bool, n)}
	for i := 0; i < n; i++ {
		s, err := NewStore(i, peers, c.Net.Transport(i), raft.NewMemoryPersister(), dirs[i])
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("start store %d: %w", i, err)
		}
		c.Net.Register(i, s.rf) // let peers reach this node's RPC handler
		c.Stores = append(c.Stores, s)
	}
	return c, nil
}

// Size returns the number of replicas.
func (c *Cluster) Size() int { return len(c.Stores) }

// Leader returns the id of a reachable node that currently believes it is
// leader, or -1. Nodes taken down are skipped: a partitioned old leader keeps
// self-reporting as leader until it rejoins, which would otherwise mislead.
func (c *Cluster) Leader() int {
	for i, s := range c.Stores {
		if !c.down[i] && s.IsLeader() {
			return i
		}
	}
	return -1
}

// WaitForLeader polls until a leader emerges or the timeout elapses.
func (c *Cluster) WaitForLeader(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id := c.Leader(); id != -1 {
			return id, nil
		}
		time.Sleep(30 * time.Millisecond)
	}
	return -1, fmt.Errorf("no leader within %s", timeout)
}

// Put replicates key=val, routing to whichever node is leader and retrying
// across leadership changes. Returns the id of the node that accepted it.
func (c *Cluster) Put(key, val []byte, timeout time.Duration) (int, error) {
	return c.mutate(func(s *Store) error { return s.Put(key, val) }, key, timeout)
}

// Delete replicates a deletion of key.
func (c *Cluster) Delete(key []byte, timeout time.Duration) (int, error) {
	return c.mutate(func(s *Store) error { return s.Delete(key) }, key, timeout)
}

func (c *Cluster) mutate(op func(*Store) error, key []byte, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, s := range c.Stores {
			if c.down[i] {
				continue // skip crashed/partitioned nodes
			}
			if err := op(s); err == nil {
				return i, nil
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return -1, fmt.Errorf("no leader accepted write for key %q within %s", key, timeout)
}

// ReadFrom reads key from a specific node's local LSM store, reflecting whatever
// that replica has applied so far.
func (c *Cluster) ReadFrom(node int, key []byte) (string, bool) {
	v, err := c.Stores[node].Get(key)
	if err != nil {
		return "", false
	}
	return string(v), true
}

// Stats returns a node's applied index, live Raft log length, and snapshot index
// — enough to observe log compaction in action.
func (c *Cluster) Stats(node int) (applied, logLen, snapIndex int) {
	s := c.Stores[node]
	return s.AppliedIndex(), s.RaftLogLength(), s.SnapshotIndex()
}

// SetDown isolates or restores a node (crash / partition simulation).
func (c *Cluster) SetDown(node int, down bool) {
	c.down[node] = down
	c.Net.SetDown(node, down)
}

// Close stops all replicas and closes their engines.
func (c *Cluster) Close() {
	for _, s := range c.Stores {
		s.Close()
	}
}
