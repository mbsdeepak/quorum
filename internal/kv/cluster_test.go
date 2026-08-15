package kv

import (
	"fmt"
	"testing"
	"time"

	"github.com/mbsdeepak/quorum/internal/lsm"
	"github.com/mbsdeepak/quorum/internal/raft"
)

// kvCluster is a 3-node replicated KV store, each node backed by its own LSM
// engine, wired over the in-memory Raft network.
type kvCluster struct {
	t      *testing.T
	n      int
	net    *raft.InMemNetwork
	stores []*Store
	dbs    []*lsm.DB
}

func makeKVCluster(t *testing.T, n int) *kvCluster {
	c := &kvCluster{t: t, n: n, net: raft.NewInMemNetwork()}
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	for i := 0; i < n; i++ {
		db, err := lsm.Open(t.TempDir(), lsm.DefaultOptions())
		if err != nil {
			t.Fatalf("open lsm %d: %v", i, err)
		}
		c.dbs = append(c.dbs, db)
		s := NewStore(i, peers, c.net.Transport(i), raft.NewMemoryPersister(), db)
		c.stores = append(c.stores, s)
		// Register the node's RPC handler so peers can reach it. (With a real
		// TCP transport this is the node binding its listen socket.)
		c.net.Register(i, s.rf)
	}
	return c
}

func (c *kvCluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.stores[i].Close()
		c.dbs[i].Close()
	}
}

// put writes through whichever node is currently leader, retrying across
// leadership changes.
func (c *kvCluster) put(key, val []byte) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			if err := c.stores[i].Put(key, val); err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no leader accepted put %q", key)
}

func (c *kvCluster) del(key []byte) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			if err := c.stores[i].Delete(key); err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no leader accepted delete %q", key)
}

// getEventually reads key from a specific node, waiting for replication to reach
// it. wantMissing asserts the key is (eventually) absent.
func (c *kvCluster) getEventually(node int, key, want []byte, wantMissing bool) {
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	var lastVal []byte
	for time.Now().Before(deadline) {
		v, err := c.dbs[node].Get(key)
		lastErr, lastVal = err, v
		if wantMissing {
			if err == lsm.ErrNotFound {
				return
			}
		} else if err == nil && string(v) == string(want) {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("node %d key %q: got (%q, %v), want missing=%v val=%q",
		node, key, lastVal, lastErr, wantMissing, want)
}

// TestReplicatedPutConverges writes through the leader and verifies every node's
// independent LSM engine ends up with the same data.
func TestReplicatedPutConverges(t *testing.T) {
	c := makeKVCluster(t, 3)
	defer c.cleanup()

	const N = 30
	for i := 0; i < N; i++ {
		c.put([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("val-%d", i)))
	}
	for node := 0; node < c.n; node++ {
		for i := 0; i < N; i++ {
			c.getEventually(node, []byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("val-%d", i)), false)
		}
	}
}

// TestReplicatedDelete verifies deletes replicate too.
func TestReplicatedDelete(t *testing.T) {
	c := makeKVCluster(t, 3)
	defer c.cleanup()

	c.put([]byte("k"), []byte("v"))
	for node := 0; node < c.n; node++ {
		c.getEventually(node, []byte("k"), []byte("v"), false)
	}
	c.del([]byte("k"))
	for node := 0; node < c.n; node++ {
		c.getEventually(node, []byte("k"), nil, true)
	}
}

// TestWriteSurvivesLeaderFailure writes, kills the leader, and confirms the data
// is still present via the surviving replicas after a new leader is elected.
func TestWriteSurvivesLeaderFailure(t *testing.T) {
	c := makeKVCluster(t, 3)
	defer c.cleanup()

	c.put([]byte("persist"), []byte("me"))

	// Find and partition the leader.
	leader := -1
	for i := 0; i < c.n; i++ {
		if c.stores[i].IsLeader() {
			leader = i
			break
		}
	}
	if leader == -1 {
		t.Fatal("no leader found")
	}
	c.net.SetDown(leader, true)

	// A surviving node must still serve the committed write and accept new ones.
	c.put([]byte("after"), []byte("failover"))
	for i := 0; i < c.n; i++ {
		if i == leader {
			continue
		}
		c.getEventually(i, []byte("persist"), []byte("me"), false)
		c.getEventually(i, []byte("after"), []byte("failover"), false)
	}
}
