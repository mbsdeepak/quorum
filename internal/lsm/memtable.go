package lsm

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	maxHeight = 16
	pBranch   = 0.25
)

// memtable is an in-memory, sorted, concurrency-safe skiplist holding the most
// recent writes. Deletes are stored as tombstones (kindDelete) so they shadow
// older values still living in SSTables, until compaction drops them.
//
// A skiplist gives O(log n) ordered insert/lookup without the rebalancing of a
// tree, and iterating the bottom level yields keys in sorted order — exactly
// what the SSTable writer needs at flush time.
type memtable struct {
	mu     sync.RWMutex
	head   *node
	height int
	rnd    *rand.Rand
	size   int64 // approximate live bytes, used to decide when to flush
}

type node struct {
	key  []byte
	val  []byte
	kind recordKind
	next []*node
}

func newMemtable() *memtable {
	return &memtable{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		// Fixed seed: the RNG only affects skiplist balance, never correctness,
		// and determinism keeps tests reproducible.
		rnd: rand.New(rand.NewSource(1)),
	}
}

func (m *memtable) randomHeight() int {
	h := 1
	for h < maxHeight && m.rnd.Float64() < pBranch {
		h++
	}
	return h
}

// put inserts or overwrites key with (kind, val).
func (m *memtable) put(kind recordKind, key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var prev [maxHeight]*node
	x := m.head
	for i := m.height - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		prev[i] = x
	}

	// Overwrite in place if the key already exists.
	if nxt := x.next[0]; nxt != nil && bytes.Equal(nxt.key, key) {
		m.size += int64(len(val)) - int64(len(nxt.val))
		nxt.val = val
		nxt.kind = kind
		return
	}

	h := m.randomHeight()
	if h > m.height {
		for i := m.height; i < h; i++ {
			prev[i] = m.head
		}
		m.height = h
	}
	n := &node{key: key, val: val, kind: kind, next: make([]*node, h)}
	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	m.size += int64(len(key) + len(val) + 1)
}

// get returns (value, kind, found). found=false means the key is absent from
// this memtable; kind==kindDelete means it is present, as a tombstone.
func (m *memtable) get(key []byte) ([]byte, recordKind, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	x := m.head
	for i := m.height - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}
	if n := x.next[0]; n != nil && bytes.Equal(n.key, key) {
		return n.val, n.kind, true
	}
	return nil, 0, false
}

func (m *memtable) approxSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// scan returns every entry in ascending key order, used when flushing to an
// SSTable. Keys are unique (put overwrites in place), so the output already
// satisfies the SSTable writer's precondition.
func (m *memtable) scan() []entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []entry
	for x := m.head.next[0]; x != nil; x = x.next[0] {
		out = append(out, entry{
			key:  append([]byte(nil), x.key...),
			val:  append([]byte(nil), x.val...),
			kind: x.kind,
		})
	}
	return out
}
