package netrpc

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mbsdeepak/quorum/internal/kv"
	"github.com/mbsdeepak/quorum/internal/raft"
)

// tcpCluster is a set of kv.Stores wired to each other over real TCP loopback
// via this package's transport — an actual networked cluster, in one test process.
type tcpCluster struct {
	t       *testing.T
	n       int
	stores  []*kv.Store
	servers []*Server
	trans   []*Transport
	crashed []bool
}

func makeTCPCluster(t *testing.T, n int) *tcpCluster {
	c := &tcpCluster{t: t, n: n}

	// Bind listeners first so every peer address is known and connectable before
	// any node starts dialing.
	lns := make([]net.Listener, n)
	addrs := map[int]string{}
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}

	peerIDs := make([]int, n)
	for i := range peerIDs {
		peerIDs[i] = i
	}
	c.crashed = make([]bool, n)
	for i := 0; i < n; i++ {
		tr := NewTransport(i, addrs)
		st, err := kv.NewStore(i, peerIDs, tr, raft.NewMemoryPersister(), t.TempDir())
		if err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
		c.trans = append(c.trans, tr)
		c.stores = append(c.stores, st)
		c.servers = append(c.servers, Serve(lns[i], st.Raft()))
	}
	return c
}

// crash simulates a node dying: it stops serving, tears down its consensus node,
// and drops its outbound connections.
func (c *tcpCluster) crash(i int) {
	c.servers[i].Close()
	c.stores[i].Close()
	c.trans[i].Close()
	c.crashed[i] = true
}

func (c *tcpCluster) cleanup() {
	for i := 0; i < c.n; i++ {
		if c.crashed[i] {
			continue
		}
		c.servers[i].Close()
		c.stores[i].Close()
		c.trans[i].Close()
	}
}

func (c *tcpCluster) put(key, val string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			if err := c.stores[i].Put([]byte(key), []byte(val)); err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no leader accepted put %q over TCP", key)
}

func (c *tcpCluster) getEventually(node int, key, want string) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, err := c.stores[node].Get([]byte(key)); err == nil && string(v) == want {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("node %d never saw %q=%q", node, key, want)
}

// TestTCPReplication runs a real 3-node cluster over TCP: elect a leader, write
// through it, and confirm every node's independent store converges — with all
// consensus traffic crossing actual sockets.
func TestTCPReplication(t *testing.T) {
	c := makeTCPCluster(t, 3)
	defer c.cleanup()

	const N = 20
	for i := 0; i < N; i++ {
		c.put(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}
	for node := 0; node < c.n; node++ {
		for i := 0; i < N; i++ {
			c.getEventually(node, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
		}
	}
}

// TestTCPLeaderFailover kills the leader's server and confirms the survivors
// elect a new leader and keep committing over the network.
func TestTCPLeaderFailover(t *testing.T) {
	c := makeTCPCluster(t, 3)
	defer c.cleanup()

	c.put("before", "x")

	// Find and isolate the leader by closing its listener and connections.
	leader := -1
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && leader == -1 {
		for i := 0; i < c.n; i++ {
			if c.stores[i].IsLeader() {
				leader = i
			}
		}
		if leader == -1 {
			time.Sleep(30 * time.Millisecond)
		}
	}
	if leader == -1 {
		t.Fatal("no leader")
	}
	c.crash(leader) // kill the leader entirely, not just its connections

	// A surviving node must still accept writes.
	deadline = time.Now().Add(5 * time.Second)
	wrote := false
	for time.Now().Before(deadline) && !wrote {
		for i := 0; i < c.n; i++ {
			if i == leader {
				continue
			}
			if err := c.stores[i].Put([]byte("after"), []byte("failover")); err == nil {
				wrote = true
				break
			}
		}
		if !wrote {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !wrote {
		t.Fatal("cluster did not accept writes after leader failure")
	}
	for i := 0; i < c.n; i++ {
		if i == leader {
			continue
		}
		c.getEventually(i, "after", "failover")
		c.getEventually(i, "before", "x")
	}
}
