package raft

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// cluster is an in-process Raft cluster wired over the in-memory network, with
// a collector per node that records every applied command so tests can assert
// agreement.
type cluster struct {
	t       *testing.T
	n       int
	net     *InMemNetwork
	nodes   []*Raft
	persist []*MemoryPersister
	applyCh []chan ApplyMsg

	mu      sync.Mutex
	applied []map[int][]byte // node -> (index -> command)
	down    []bool
}

func makeCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t:       t,
		n:       n,
		net:     NewInMemNetwork(),
		nodes:   make([]*Raft, n),
		persist: make([]*MemoryPersister, n),
		applyCh: make([]chan ApplyMsg, n),
		applied: make([]map[int][]byte, n),
		down:    make([]bool, n),
	}
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	for i := 0; i < n; i++ {
		c.persist[i] = NewMemoryPersister()
		c.applied[i] = make(map[int][]byte)
		c.startNode(i, peers)
	}
	return c
}

// startNode builds node i, registers it, and starts its apply collector.
func (c *cluster) startNode(i int, peers []int) {
	ch := make(chan ApplyMsg, 256)
	c.applyCh[i] = ch
	rf := Make(i, peers, c.net.Transport(i), c.persist[i], ch)
	c.nodes[i] = rf
	c.net.Register(i, rf)
	go c.collect(i, ch)
}

func (c *cluster) collect(i int, ch chan ApplyMsg) {
	for m := range ch {
		if !m.CommandValid {
			continue
		}
		c.mu.Lock()
		// Cross-node agreement: same index must carry the same command.
		for j := 0; j < c.n; j++ {
			if prev, ok := c.applied[j][m.CommandIndex]; ok && !bytes.Equal(prev, m.Command) {
				c.mu.Unlock()
				c.t.Fatalf("apply mismatch at index %d: node %d=%q vs node %d=%q",
					m.CommandIndex, j, prev, i, m.Command)
				return
			}
		}
		c.applied[i][m.CommandIndex] = append([]byte(nil), m.Command...)
		c.mu.Unlock()
	}
}

func (c *cluster) cleanup() {
	for i := 0; i < c.n; i++ {
		if c.nodes[i] != nil {
			c.nodes[i].Kill()
		}
	}
}

func (c *cluster) setDown(i int, down bool) {
	c.mu.Lock()
	c.down[i] = down
	c.mu.Unlock()
	c.net.SetDown(i, down)
}

// crashRestart kills node i and immediately restarts it from the SAME persister,
// simulating a reboot that must recover durable state.
func (c *cluster) crashRestart(i int) {
	c.nodes[i].Kill()
	peers := make([]int, c.n)
	for j := range peers {
		peers[j] = j
	}
	c.startNode(i, peers)
}

// checkOneLeader returns the index of the sole leader among reachable nodes,
// retrying to let an election settle.
func (c *cluster) checkOneLeader() int {
	for iter := 0; iter < 12; iter++ {
		time.Sleep(250 * time.Millisecond)
		leadersByTerm := map[int][]int{}
		for i := 0; i < c.n; i++ {
			c.mu.Lock()
			down := c.down[i]
			c.mu.Unlock()
			if down {
				continue
			}
			if term, isLeader, _ := c.nodes[i].Status(); isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], i)
			}
		}
		lastTerm := -1
		for term := range leadersByTerm {
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm >= 0 {
			if got := len(leadersByTerm[lastTerm]); got != 1 {
				c.t.Fatalf("term %d has %d leaders, want 1", lastTerm, got)
			}
			return leadersByTerm[lastTerm][0]
		}
	}
	c.t.Fatalf("no leader elected")
	return -1
}

func (c *cluster) checkNoLeader() {
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		down := c.down[i]
		c.mu.Unlock()
		if down {
			continue
		}
		if _, isLeader, _ := c.nodes[i].Status(); isLeader {
			c.t.Fatalf("node %d is leader but a majority is down", i)
		}
	}
}

// submit finds the current leader and proposes cmd, retrying across leadership
// changes. Returns the log index the command was assigned.
func (c *cluster) submit(cmd []byte) int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			c.mu.Lock()
			down := c.down[i]
			c.mu.Unlock()
			if down {
				continue
			}
			if idx, _, ok := c.nodes[i].Start(cmd); ok {
				return idx
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no leader accepted command %q", cmd)
	return -1
}

// waitApplied blocks until at least want nodes have applied index, then returns
// the agreed command.
func (c *cluster) waitApplied(index, want int) []byte {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		var cmd []byte
		count := 0
		for i := 0; i < c.n; i++ {
			if v, ok := c.applied[i][index]; ok {
				count++
				cmd = v
			}
		}
		c.mu.Unlock()
		if count >= want {
			return cmd
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("index %d applied on too few nodes", index)
	return nil
}

// ---- tests ----------------------------------------------------------------

func TestElectionOneLeader(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()
	c.checkOneLeader()
}

func TestReElectionAfterLeaderFailure(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leader := c.checkOneLeader()
	c.setDown(leader, true) // partition the leader away
	newLeader := c.checkOneLeader()
	if newLeader == leader {
		t.Fatalf("expected a different leader after failure")
	}
	// The old leader rejoins and must accept the new leader's authority.
	c.setDown(leader, false)
	c.checkOneLeader()
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leader := c.checkOneLeader()
	// Down two of three: no majority can be formed, so no leader may exist.
	c.setDown(leader, true)
	c.setDown((leader+1)%3, true)
	c.checkNoLeader()
}

func TestLogReplicationAgreement(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()
	c.checkOneLeader()

	for i := 0; i < 20; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		idx := c.submit(cmd)
		got := c.waitApplied(idx, c.n) // committed on ALL three
		if !bytes.Equal(got, cmd) {
			t.Fatalf("index %d applied %q, want %q", idx, got, cmd)
		}
	}
}

func TestReplicationWithOneFollowerDown(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()

	leader := c.checkOneLeader()
	follower := (leader + 1) % 3
	c.setDown(follower, true) // one follower offline; a 2/3 quorum remains

	var last int
	for i := 0; i < 10; i++ {
		last = c.submit([]byte(fmt.Sprintf("v%d", i)))
		c.waitApplied(last, 2) // majority commits without the down node
	}

	// Follower rejoins and must catch up to the full log.
	c.setDown(follower, false)
	c.waitApplied(last, 3)
}

func TestPersistenceAcrossRestart(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.cleanup()
	c.checkOneLeader()

	idx := c.submit([]byte("durable"))
	c.waitApplied(idx, c.n)

	// Reboot one node; it must recover its log from the persister and re-apply.
	c.crashRestart(1)
	c.checkOneLeader()
	got := c.waitApplied(idx, 1) // node 1 re-applies from recovered state
	if !bytes.Equal(got, []byte("durable")) {
		t.Fatalf("after restart, index %d = %q, want %q", idx, got, "durable")
	}
}
