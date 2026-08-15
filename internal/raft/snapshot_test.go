package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"
)

// snapCluster is a Raft cluster whose state machine is an in-memory key/value
// map. Each node snapshots every snapEvery applied commands and rebuilds its map
// when Raft delivers a snapshot — exactly the contract a real application meets.
type snapCluster struct {
	t         *testing.T
	n         int
	net       *InMemNetwork
	rafts     []*Raft
	persist   []*MemoryPersister
	snapEvery int

	mu   sync.Mutex
	kv   []map[string]string
	down []bool
}

func makeSnapCluster(t *testing.T, n, snapEvery int) *snapCluster {
	c := &snapCluster{
		t:         t,
		n:         n,
		net:       NewInMemNetwork(),
		rafts:     make([]*Raft, n),
		persist:   make([]*MemoryPersister, n),
		snapEvery: snapEvery,
		kv:        make([]map[string]string, n),
		down:      make([]bool, n),
	}
	for i := 0; i < n; i++ {
		c.persist[i] = NewMemoryPersister()
		c.kv[i] = map[string]string{}
		c.startNode(i)
	}
	return c
}

func (c *snapCluster) startNode(i int) {
	peers := make([]int, c.n)
	for j := range peers {
		peers[j] = j
	}
	ch := make(chan ApplyMsg, 512)
	rf := Make(i, peers, c.net.Transport(i), c.persist[i], ch)
	c.rafts[i] = rf
	c.net.Register(i, rf)
	go c.apply(i, rf, ch)
}

// apply consumes committed commands and snapshots for node i.
func (c *snapCluster) apply(i int, rf *Raft, ch chan ApplyMsg) {
	for m := range ch {
		switch {
		case m.SnapshotValid:
			c.mu.Lock()
			c.kv[i] = decodeKVMap(m.Snapshot)
			c.mu.Unlock()
		case m.CommandValid:
			k, v := splitCmd(m.Command)
			c.mu.Lock()
			c.kv[i][k] = v
			snap := encodeKVMap(c.kv[i])
			c.mu.Unlock()
			if c.snapEvery > 0 && m.CommandIndex%c.snapEvery == 0 {
				rf.Snapshot(m.CommandIndex, snap)
			}
		}
	}
}

func (c *snapCluster) cleanup() {
	for _, rf := range c.rafts {
		if rf != nil {
			rf.Kill()
		}
	}
}

func (c *snapCluster) setDown(i int, down bool) {
	c.mu.Lock()
	c.down[i] = down
	c.mu.Unlock()
	c.net.SetDown(i, down)
}

func (c *snapCluster) crashRestart(i int) {
	c.rafts[i].Kill()
	c.mu.Lock()
	c.kv[i] = map[string]string{} // volatile SM loses memory; must rebuild
	c.mu.Unlock()
	c.startNode(i)
}

func (c *snapCluster) leader() int {
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		down := c.down[i]
		c.mu.Unlock()
		if down {
			continue
		}
		if _, isLeader, _ := c.rafts[i].Status(); isLeader {
			return i
		}
	}
	return -1
}

func (c *snapCluster) waitLeader() int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != -1 {
			return l
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected")
	return -1
}

// submit proposes key=val via the current leader, retrying across changes.
func (c *snapCluster) submit(key, val string) {
	cmd := []byte(key + "=" + val)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			c.mu.Lock()
			down := c.down[i]
			c.mu.Unlock()
			if down {
				continue
			}
			if _, _, ok := c.rafts[i].Start(cmd); ok {
				return
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	c.t.Fatalf("no leader accepted %s=%s", key, val)
}

// waitKV blocks until node i's state machine shows key=val.
func (c *snapCluster) waitKV(i int, key, val string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := c.kv[i][key]
		c.mu.Unlock()
		if got == val {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.mu.Lock()
	got := c.kv[i][key]
	c.mu.Unlock()
	c.t.Fatalf("node %d: key %q = %q, want %q", i, key, got, val)
}

// ---- tests ----------------------------------------------------------------

// TestSnapshotCompactsLog verifies the in-memory log actually shrinks as the
// application snapshots, while all replicas keep the same state.
func TestSnapshotCompactsLog(t *testing.T) {
	c := makeSnapCluster(t, 3, 10)
	defer c.cleanup()
	c.waitLeader()

	const N = 60
	for i := 0; i < N; i++ {
		c.submit(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	// All nodes converge on the last write...
	for i := 0; i < c.n; i++ {
		c.waitKV(i, fmt.Sprintf("k%d", N-1), fmt.Sprintf("v%d", N-1))
	}
	// ...and the log has been compacted well below the number of commands.
	time.Sleep(300 * time.Millisecond)
	for i := 0; i < c.n; i++ {
		if got := c.rafts[i].LogLength(); got > 20 {
			t.Fatalf("node %d log length %d, expected compaction below ~20", i, got)
		}
		if snap := c.rafts[i].SnapshotIndex(); snap == 0 {
			t.Fatalf("node %d never took a snapshot", i)
		}
	}
}

// TestSnapshotCatchUpLaggingFollower is the key snapshot scenario: a follower
// misses so many entries that the leader has compacted them, so catch-up MUST
// happen via InstallSnapshot rather than the log.
func TestSnapshotCatchUpLaggingFollower(t *testing.T) {
	c := makeSnapCluster(t, 3, 10)
	defer c.cleanup()
	leader := c.waitLeader()
	follower := (leader + 1) % 3

	c.setDown(follower, true) // partition one follower away

	// Enough writes that the leader snapshots past where the follower stopped.
	const N = 80
	for i := 0; i < N; i++ {
		c.submit(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	// The leader's log is compacted, so the missed entries no longer exist there.
	if snap := c.rafts[leader].SnapshotIndex(); snap < 10 {
		t.Fatalf("leader did not compact (snapshot index %d)", snap)
	}

	c.setDown(follower, false) // follower rejoins; must be caught up by snapshot
	c.waitKV(follower, fmt.Sprintf("k%d", N-1), fmt.Sprintf("v%d", N-1))
	c.waitKV(follower, "k0", "v0")
	if snap := c.rafts[follower].SnapshotIndex(); snap == 0 {
		t.Fatalf("follower was not caught up via a snapshot")
	}
}

// TestSnapshotRestart verifies a rebooted node rebuilds its state machine from
// the persisted snapshot (plus any log tail).
func TestSnapshotRestart(t *testing.T) {
	c := makeSnapCluster(t, 3, 10)
	defer c.cleanup()
	c.waitLeader()

	const N = 45
	for i := 0; i < N; i++ {
		c.submit(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	for i := 0; i < c.n; i++ {
		c.waitKV(i, fmt.Sprintf("k%d", N-1), fmt.Sprintf("v%d", N-1))
	}

	c.crashRestart(1) // node 1 reboots with an empty in-memory map
	c.waitLeader()
	// It must recover early and late keys from snapshot + replayed tail.
	c.waitKV(1, "k0", "v0")
	c.waitKV(1, fmt.Sprintf("k%d", N-1), fmt.Sprintf("v%d", N-1))
}

// ---- state-machine codec --------------------------------------------------

func encodeKVMap(m map[string]string) []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(m)
	return buf.Bytes()
}

func decodeKVMap(b []byte) map[string]string {
	m := map[string]string{}
	if len(b) > 0 {
		_ = gob.NewDecoder(bytes.NewReader(b)).Decode(&m)
	}
	return m
}

func splitCmd(b []byte) (string, string) {
	s := string(b)
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}
