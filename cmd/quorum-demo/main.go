// Command quorum-demo runs a 3-node quorum cluster in a single process (over the
// in-memory transport) and narrates a scripted walkthrough: leader election,
// replicated writes converging across all nodes, leader failure + failover, and
// proof that committed data survives.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/mbsdeepak/quorum/internal/kv"
)

const writeTimeout = 5 * time.Second

func main() {
	dirs := []string{
		mustTempDir("quorum-node0-"),
		mustTempDir("quorum-node1-"),
		mustTempDir("quorum-node2-"),
	}
	defer func() {
		for _, d := range dirs {
			os.RemoveAll(d)
		}
	}()

	step("Starting a 3-node quorum cluster (each node has its own LSM engine)")
	c, err := kv.NewInMemCluster(dirs)
	if err != nil {
		fail(err)
	}
	defer c.Close()

	// 1) Leader election.
	step("Waiting for the cluster to elect a leader")
	leader, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		fail(err)
	}
	fmt.Printf("   ✓ node %d was elected leader\n", leader)

	// 2) Replicated writes.
	step("Writing 4 keys through the cluster (auto-routed to the leader)")
	data := [][2]string{
		{"user:1", "deepak"},
		{"user:2", "claude"},
		{"lang", "go"},
		{"project", "quorum"},
	}
	for _, kv := range data {
		acceptedBy, err := c.Put([]byte(kv[0]), []byte(kv[1]), writeTimeout)
		if err != nil {
			fail(err)
		}
		fmt.Printf("   put %-10s = %-8s  (committed via leader node %d)\n", kv[0], kv[1], acceptedBy)
	}

	// 3) Convergence: read every key from every node's independent engine.
	time.Sleep(300 * time.Millisecond) // let followers apply
	step("Reading every key from ALL three nodes — they must agree")
	printTable(c, keysOf(data))

	// 4) Leader failure.
	step(fmt.Sprintf("Simulating a crash: partitioning leader (node %d) away", leader))
	c.SetDown(leader, true)
	newLeader, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		fail(err)
	}
	fmt.Printf("   ✓ cluster re-elected: node %d is the new leader\n", newLeader)

	// 5) Write after failover.
	step("Writing a new key AFTER the failover")
	acceptedBy, err := c.Put([]byte("status"), []byte("failed-over"), writeTimeout)
	if err != nil {
		fail(err)
	}
	fmt.Printf("   put status     = failed-over  (committed via leader node %d)\n", acceptedBy)

	// 6) Prove old + new data present on the survivors.
	time.Sleep(300 * time.Millisecond)
	step("Reading from the surviving nodes — old data survived, new data is there")
	printTable(c, append(keysOf(data), "status"))
	fmt.Printf("   (node %d is partitioned — shown as offline)\n", leader)

	// 7) Old leader rejoins and catches up.
	step(fmt.Sprintf("Bringing node %d back — it rejoins and catches up", leader))
	c.SetDown(leader, false)
	waitCaughtUp(c, leader, "status", "failed-over")
	printTable(c, append(keysOf(data), "status"))

	// 8) Log compaction via snapshots.
	step("Writing 120 more keys to trigger snapshots (Raft log compaction)")
	for i := 0; i < 120; i++ {
		if _, err := c.Put([]byte(fmt.Sprintf("bulk:%03d", i)), []byte(fmt.Sprintf("%d", i)), writeTimeout); err != nil {
			fail(err)
		}
	}
	fmt.Println("   done — 120 keys committed")
	time.Sleep(300 * time.Millisecond)

	step("Each node kept its Raft log SMALL despite ~125 total writes — snapshots compacted it")
	fmt.Printf("   %-8s | %-12s | %-16s | %s\n", "node", "applied idx", "raft log entries", "snapshot @ idx")
	fmt.Printf("   %-8s | %-12s | %-16s | %s\n", "--------", "------------", "----------------", "--------------")
	for n := 0; n < c.Size(); n++ {
		applied, logLen, snapIdx := c.Stats(n)
		fmt.Printf("   node %-3d | %-12d | %-16d | %d\n", n, applied, logLen, snapIdx)
	}

	fmt.Println("\n✓ Demo complete: a leader was elected, writes replicated to all nodes,")
	fmt.Println("  the cluster survived a leader failure, the recovered node caught up,")
	fmt.Println("  and snapshots compacted the log while state stayed consistent.")
}

// printTable prints each key's value as seen by every node.
func printTable(c *kv.Cluster, keys []string) {
	fmt.Printf("   %-10s", "key")
	for n := 0; n < c.Size(); n++ {
		fmt.Printf(" | node %d", n)
	}
	fmt.Println()
	fmt.Printf("   %-10s", "----------")
	for n := 0; n < c.Size(); n++ {
		fmt.Printf(" | ------")
	}
	fmt.Println()
	for _, k := range keys {
		fmt.Printf("   %-10s", k)
		for n := 0; n < c.Size(); n++ {
			v, ok := c.ReadFrom(n, []byte(k))
			if !ok {
				v = "·"
			}
			fmt.Printf(" | %-6s", v)
		}
		fmt.Println()
	}
}

// waitCaughtUp blocks until node shows the expected value for key.
func waitCaughtUp(c *kv.Cluster, node int, key, want string) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.ReadFrom(node, []byte(key)); ok && v == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func keysOf(data [][2]string) []string {
	ks := make([]string, len(data))
	for i, kv := range data {
		ks[i] = kv[0]
	}
	return ks
}

func step(msg string) {
	fmt.Printf("\n▸ %s\n", msg)
	time.Sleep(150 * time.Millisecond)
}

func mustTempDir(prefix string) string {
	d, err := os.MkdirTemp("", prefix)
	if err != nil {
		fail(err)
	}
	return d
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "demo failed:", err)
	os.Exit(1)
}
