package raft

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// memberCluster supports adding and removing nodes at runtime, to exercise
// configuration changes.
type memberCluster struct {
	t   *testing.T
	net *InMemNetwork

	mu      sync.Mutex
	rafts   map[int]*Raft
	persist map[int]*MemoryPersister
	applied map[int]map[int][]byte // node -> index -> command
}

func newMemberCluster(t *testing.T) *memberCluster {
	return &memberCluster{
		t:       t,
		net:     NewInMemNetwork(),
		rafts:   map[int]*Raft{},
		persist: map[int]*MemoryPersister{},
		applied: map[int]map[int][]byte{},
	}
}

// start brings up node id with an initial config. A brand-new node being added
// to an existing cluster passes an empty config so it stays a passive learner
// (never campaigns) until it learns the real config from the leader.
func (c *memberCluster) start(id int, config []int) {
	c.mu.Lock()
	c.persist[id] = NewMemoryPersister()
	c.applied[id] = map[int][]byte{}
	c.mu.Unlock()

	ch := make(chan ApplyMsg, 512)
	rf := Make(id, config, c.net.Transport(id), c.persist[id], ch)
	c.mu.Lock()
	c.rafts[id] = rf
	c.mu.Unlock()
	c.net.Register(id, rf)
	go c.collect(id, ch)
}

func (c *memberCluster) collect(id int, ch chan ApplyMsg) {
	for m := range ch {
		if !m.CommandValid {
			continue
		}
		c.mu.Lock()
		c.applied[id][m.CommandIndex] = append([]byte(nil), m.Command...)
		c.mu.Unlock()
	}
}

func (c *memberCluster) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rf := range c.rafts {
		rf.Kill()
	}
}

func (c *memberCluster) ids() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int
	for id := range c.rafts {
		out = append(out, id)
	}
	return out
}

func (c *memberCluster) leaderRaft() *Raft {
	c.mu.Lock()
	rafts := make([]*Raft, 0, len(c.rafts))
	for _, rf := range c.rafts {
		rafts = append(rafts, rf)
	}
	c.mu.Unlock()
	for _, rf := range rafts {
		if _, isLeader, _ := rf.Status(); isLeader {
			return rf
		}
	}
	return nil
}

func (c *memberCluster) waitLeader() *Raft {
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if rf := c.leaderRaft(); rf != nil {
			return rf
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return nil
}

// submit proposes cmd through the current leader, returning the assigned index.
func (c *memberCluster) submit(cmd []byte) int {
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if rf := c.leaderRaft(); rf != nil {
			if idx, _, ok := rf.Start(cmd); ok {
				return idx
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	c.t.Fatalf("no leader accepted %q", cmd)
	return -1
}

// changeConfig proposes newConfig via the leader, retrying while a change is
// still pending.
func (c *memberCluster) changeConfig(newConfig []int) {
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if rf := c.leaderRaft(); rf != nil {
			if _, ok := rf.ChangeConfig(newConfig); ok {
				return
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	c.t.Fatalf("config change to %v not accepted", newConfig)
}

// waitAppliedOn blocks until node id has applied index with the given command.
func (c *memberCluster) waitAppliedOn(id, index int, want []byte) {
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got, ok := c.applied[id][index]
		c.mu.Unlock()
		if ok && bytes.Equal(got, want) {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("node %d never applied index %d = %q", id, index, want)
}

func (c *memberCluster) configOf(id int) []int {
	c.mu.Lock()
	rf := c.rafts[id]
	c.mu.Unlock()
	return rf.Config()
}

// ---- tests ----------------------------------------------------------------

// TestMembershipAddServer grows a 3-node cluster to 4 and verifies the new node
// catches up and participates.
func TestMembershipAddServer(t *testing.T) {
	c := newMemberCluster(t)
	defer c.cleanup()
	for _, id := range []int{0, 1, 2} {
		c.start(id, []int{0, 1, 2})
	}
	c.waitLeader()
	idx := c.submit([]byte("before-add"))

	// Bring up node 3 as a learner, then add it to the configuration.
	c.start(3, []int{})
	c.changeConfig([]int{0, 1, 2, 3})

	// The new node must catch up on the pre-add entry and new writes.
	c.waitAppliedOn(3, idx, []byte("before-add"))
	idx2 := c.submit([]byte("after-add"))
	for _, id := range []int{0, 1, 2, 3} {
		c.waitAppliedOn(id, idx2, []byte("after-add"))
	}
	if got := c.configOf(3); len(got) != 4 {
		t.Fatalf("node 3 config = %v, want 4 members", got)
	}
}

// TestMembershipRemoveFollower shrinks a 3-node cluster by removing a follower.
func TestMembershipRemoveFollower(t *testing.T) {
	c := newMemberCluster(t)
	defer c.cleanup()
	for _, id := range []int{0, 1, 2} {
		c.start(id, []int{0, 1, 2})
	}
	leader := c.waitLeader()
	leaderID := leader.id

	// Pick a follower to remove.
	var follower int
	for _, id := range []int{0, 1, 2} {
		if id != leaderID {
			follower = id
			break
		}
	}
	remaining := []int{}
	for _, id := range []int{0, 1, 2} {
		if id != follower {
			remaining = append(remaining, id)
		}
	}

	c.changeConfig(remaining)

	// The surviving members still commit new writes.
	idx := c.submit([]byte("after-remove"))
	for _, id := range remaining {
		c.waitAppliedOn(id, idx, []byte("after-remove"))
	}
	// The removed node learned it is gone and dropped out of the config.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && contains(c.configOf(follower), follower) {
		time.Sleep(30 * time.Millisecond)
	}
	if contains(c.configOf(follower), follower) {
		t.Fatalf("removed node %d still thinks it is a member: %v", follower, c.configOf(follower))
	}
}

// TestMembershipRemoveLeader removes the current leader and verifies the rest
// elect a new one and keep committing.
func TestMembershipRemoveLeader(t *testing.T) {
	c := newMemberCluster(t)
	defer c.cleanup()
	for _, id := range []int{0, 1, 2} {
		c.start(id, []int{0, 1, 2})
	}
	leader := c.waitLeader()
	leaderID := leader.id

	remaining := []int{}
	for _, id := range []int{0, 1, 2} {
		if id != leaderID {
			remaining = append(remaining, id)
		}
	}
	c.changeConfig(remaining)

	// A new leader must emerge among the remaining nodes and commit writes.
	deadline := time.Now().Add(5 * time.Second)
	var newLeader *Raft
	for time.Now().Before(deadline) {
		if rf := c.leaderRaft(); rf != nil && rf.id != leaderID {
			newLeader = rf
			break
		}
		time.Sleep(40 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader after removing the old one")
	}
	idx := c.submit([]byte("post-leader-removal"))
	for _, id := range remaining {
		c.waitAppliedOn(id, idx, []byte("post-leader-removal"))
	}
}

func contains(s []int, x int) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}
