// Command quorum-server runs one node of a quorum cluster as a real process:
// Raft consensus over TCP (net/rpc) to its peers, plus a small HTTP API for
// clients. Run three of these on different ports and you have a live, replicated,
// fault-tolerant key-value store.
//
// Example (three nodes on one machine):
//
//	peers="0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002"
//	quorum-server -id 0 -raft 127.0.0.1:9000 -http 127.0.0.1:8000 -peers "$peers" -dir /tmp/q0
//	quorum-server -id 1 -raft 127.0.0.1:9001 -http 127.0.0.1:8001 -peers "$peers" -dir /tmp/q1
//	quorum-server -id 2 -raft 127.0.0.1:9002 -http 127.0.0.1:8002 -peers "$peers" -dir /tmp/q2
//
//	curl -X PUT  127.0.0.1:8000/kv/hello -d world
//	curl        127.0.0.1:8000/kv/hello        # -> world
//	curl -X DELETE 127.0.0.1:8000/kv/hello
//
// Writes must go to the leader; a follower answers 421 with the hint to retry.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/mbsdeepak/quorum/internal/kv"
	"github.com/mbsdeepak/quorum/internal/lsm"
	"github.com/mbsdeepak/quorum/internal/netrpc"
	"github.com/mbsdeepak/quorum/internal/raft"
)

func main() {
	id := flag.Int("id", 0, "this node's id")
	raftAddr := flag.String("raft", "127.0.0.1:9000", "address to serve Raft RPCs on")
	httpAddr := flag.String("http", "127.0.0.1:8000", "address to serve the client HTTP API on")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port for every node, including this one")
	dir := flag.String("dir", "", "data directory (required)")
	flag.Parse()

	if *dir == "" {
		log.Fatal("-dir is required")
	}
	peers, ids, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("bad -peers: %v", err)
	}

	// Serve Raft RPCs to peers.
	ln, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		log.Fatalf("listen raft: %v", err)
	}
	trans := netrpc.NewTransport(*id, peers)
	persister := raft.NewFilePersister(
		filepath.Join(*dir, "raft.state"),
		filepath.Join(*dir, "raft.snap"),
	)
	store, err := kv.NewStore(*id, ids, trans, persister, *dir)
	if err != nil {
		log.Fatalf("start store: %v", err)
	}
	netrpc.Serve(ln, store.Raft())
	log.Printf("node %d: raft on %s, http on %s, peers=%v", *id, *raftAddr, *httpAddr, peers)

	// Serve the client HTTP API.
	http.HandleFunc("/kv/", kvHandler(store))
	http.HandleFunc("/status", statusHandler(store, *id))
	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}

func kvHandler(store *kv.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			v, err := store.Get([]byte(key))
			if errors.Is(err, lsm.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(v)
		case http.MethodPut, http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			writeResult(w, store.Put([]byte(key), body))
		case http.MethodDelete:
			writeResult(w, store.Delete([]byte(key)))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func writeResult(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok\n")
	case errors.Is(err, kv.ErrNotLeader):
		http.Error(w, "not leader — retry on the leader", http.StatusMisdirectedRequest) // 421
	default:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

func statusHandler(store *kv.Store, id int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "node %d\nleader: %v\napplied: %d\n", id, store.IsLeader(), store.AppliedIndex())
	}
}

func parsePeers(s string) (map[int]string, []int, error) {
	peers := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			return nil, nil, fmt.Errorf("expected id=addr, got %q", part)
		}
		id, err := strconv.Atoi(part[:eq])
		if err != nil {
			return nil, nil, fmt.Errorf("bad id in %q", part)
		}
		peers[id] = part[eq+1:]
	}
	if len(peers) == 0 {
		return nil, nil, errors.New("no peers given")
	}
	ids := make([]int, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return peers, ids, nil
}
