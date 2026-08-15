package raft

import "sync"

// Transport is how a node reaches its peers. It is deliberately narrow: two
// RPCs, each returning ok=false when the peer is unreachable (crashed or
// partitioned). This is the seam where an in-memory network (tests) or a real
// TCP/HTTP transport (Phase 3) plugs in.
type Transport interface {
	SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	SendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
}

// rpcHandler is the receiving side a node registers with the network.
type rpcHandler interface {
	HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply)
	HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)
}

// InMemNetwork routes RPCs between in-process Raft nodes and can drop traffic to
// or from any node to simulate crashes and partitions — enough to exercise
// elections, re-elections, and log catch-up deterministically without sockets.
type InMemNetwork struct {
	mu       sync.RWMutex
	handlers map[int]rpcHandler
	down     map[int]bool
}

func NewInMemNetwork() *InMemNetwork {
	return &InMemNetwork{
		handlers: make(map[int]rpcHandler),
		down:     make(map[int]bool),
	}
}

// Register attaches a node's handler under its id.
func (n *InMemNetwork) Register(id int, h rpcHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// SetDown isolates (or restores) a node. A down node can neither send nor
// receive, modelling a crash or a network partition.
func (n *InMemNetwork) SetDown(id int, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
}

// Transport returns a sender bound to node `from`.
func (n *InMemNetwork) Transport(from int) Transport {
	return &inMemTransport{net: n, from: from}
}

// reachable reports whether from can currently talk to to, and returns to's
// handler if so.
func (n *InMemNetwork) reachable(from, to int) (rpcHandler, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.down[from] || n.down[to] {
		return nil, false
	}
	h, ok := n.handlers[to]
	return h, ok
}

type inMemTransport struct {
	net  *InMemNetwork
	from int
}

func (t *inMemTransport) SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	h, ok := t.net.reachable(t.from, peer)
	if !ok {
		return false
	}
	// Dispatch synchronously: the caller never holds its own lock here, and the
	// handler locks only the *target* node, so no cross-node lock cycle exists.
	h.HandleRequestVote(args, reply)
	return true
}

func (t *inMemTransport) SendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	h, ok := t.net.reachable(t.from, peer)
	if !ok {
		return false
	}
	h.HandleAppendEntries(args, reply)
	return true
}
