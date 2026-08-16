// Package netrpc is a real networked transport for Raft, over TCP using Go's
// net/rpc (gob-encoded). It replaces the in-memory transport used in tests so
// quorum nodes can run as separate processes on separate machines.
//
// Each node runs a Server (accepting RequestVote / AppendEntries /
// InstallSnapshot on a TCP listener) and a Transport (a lazily-dialed, pooled
// client for reaching peers). The Transport satisfies raft.Transport, so the
// consensus core is unchanged — only the wire underneath it is different.
package netrpc

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/mbsdeepak/quorum/internal/raft"
)

// Handler is the receive side of Raft RPCs — satisfied by *raft.Raft.
type Handler interface {
	HandleRequestVote(*raft.RequestVoteArgs, *raft.RequestVoteReply)
	HandleAppendEntries(*raft.AppendEntriesArgs, *raft.AppendEntriesReply)
	HandleInstallSnapshot(*raft.InstallSnapshotArgs, *raft.InstallSnapshotReply)
}

// service adapts a Handler to net/rpc's `Method(args, *reply) error` shape.
type service struct{ h Handler }

func (s *service) RequestVote(a *raft.RequestVoteArgs, r *raft.RequestVoteReply) error {
	s.h.HandleRequestVote(a, r)
	return nil
}

func (s *service) AppendEntries(a *raft.AppendEntriesArgs, r *raft.AppendEntriesReply) error {
	s.h.HandleAppendEntries(a, r)
	return nil
}

func (s *service) InstallSnapshot(a *raft.InstallSnapshotArgs, r *raft.InstallSnapshotReply) error {
	s.h.HandleInstallSnapshot(a, r)
	return nil
}

// Server accepts Raft RPCs for one node on a TCP listener.
type Server struct {
	ln  net.Listener
	srv *rpc.Server
}

// Serve registers h under the "Raft" service and starts accepting on ln. A fresh
// rpc.Server (not the global default) is used so multiple nodes can coexist in
// one process without service-name collisions.
func Serve(ln net.Listener, h Handler) *Server {
	srv := rpc.NewServer()
	// RegisterName only errors if the type exposes no suitable methods; ours do.
	_ = srv.RegisterName("Raft", &service{h: h})
	s := &Server{ln: ln, srv: srv}
	go s.acceptLoop()
	return s
}

// acceptLoop serves each connection, and returns quietly once the listener is
// closed (unlike rpc.Server.Accept, which logs the resulting error).
func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go s.srv.ServeConn(conn)
	}
}

// Close stops accepting new connections.
func (s *Server) Close() error { return s.ln.Close() }

// Transport dials peers by id and satisfies raft.Transport. Connections are
// pooled (net/rpc clients are safe for concurrent calls) and redialed on error.
type Transport struct {
	self  int
	peers map[int]string // id -> "host:port"

	mu    sync.Mutex
	conns map[int]*rpc.Client
}

// NewTransport builds a transport for node self. peers maps every node id
// (including self, which is never dialed) to its RPC address.
func NewTransport(self int, peers map[int]string) *Transport {
	cp := make(map[int]string, len(peers))
	for k, v := range peers {
		cp[k] = v
	}
	return &Transport{self: self, peers: cp, conns: map[int]*rpc.Client{}}
}

// SetPeer adds or updates a peer address (used when the cluster grows).
func (t *Transport) SetPeer(id int, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addr
}

func (t *Transport) client(peer int) *rpc.Client {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return c
	}
	addr, ok := t.peers[peer]
	if !ok {
		return nil
	}
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil // peer down; caller treats as unreachable
	}
	t.conns[peer] = c
	return c
}

func (t *Transport) drop(peer int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		c.Close()
		delete(t.conns, peer)
	}
}

// call returns false when the peer is unreachable — exactly the semantics Raft
// expects from a dropped/timed-out RPC.
func (t *Transport) call(peer int, method string, args, reply any) bool {
	c := t.client(peer)
	if c == nil {
		return false
	}
	if err := c.Call("Raft."+method, args, reply); err != nil {
		t.drop(peer) // stale connection; force a redial next time
		return false
	}
	return true
}

func (t *Transport) SendRequestVote(peer int, a *raft.RequestVoteArgs, r *raft.RequestVoteReply) bool {
	return t.call(peer, "RequestVote", a, r)
}

func (t *Transport) SendAppendEntries(peer int, a *raft.AppendEntriesArgs, r *raft.AppendEntriesReply) bool {
	return t.call(peer, "AppendEntries", a, r)
}

func (t *Transport) SendInstallSnapshot(peer int, a *raft.InstallSnapshotArgs, r *raft.InstallSnapshotReply) bool {
	return t.call(peer, "InstallSnapshot", a, r)
}

// Close tears down all pooled connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Close()
	}
	t.conns = map[int]*rpc.Client{}
}
