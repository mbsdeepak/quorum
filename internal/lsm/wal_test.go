package lsm

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestWALReplay(t *testing.T) {
	p := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	must(t, w.Append(kindPut, []byte("a"), []byte("1")))
	must(t, w.Append(kindPut, []byte("b"), []byte("2")))
	must(t, w.Append(kindDelete, []byte("a"), nil))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []walEntry
	if err := ReplayWAL(p, func(e walEntry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 records, got %d", len(got))
	}
	if string(got[0].key) != "a" || string(got[0].val) != "1" || got[0].kind != kindPut {
		t.Fatalf("bad record 0: %+v", got[0])
	}
	if got[2].kind != kindDelete || string(got[2].key) != "a" {
		t.Fatalf("bad record 2: %+v", got[2])
	}
}

func TestWALReplayMissingFile(t *testing.T) {
	// Replaying a non-existent log is a clean no-op (fresh database).
	n := 0
	if err := ReplayWAL(filepath.Join(t.TempDir(), "nope.log"), func(walEntry) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 records, got %d", n)
	}
}

func TestBloomNoFalseNegatives(t *testing.T) {
	b := newBloom(1000, 0.01)
	for i := 0; i < 1000; i++ {
		b.add([]byte(fmt.Sprintf("k%d", i)))
	}
	for i := 0; i < 1000; i++ {
		if !b.mayContain([]byte(fmt.Sprintf("k%d", i))) {
			t.Fatalf("false negative for k%d", i)
		}
	}
	// Round-trip through encode/decode must preserve membership.
	b2 := decodeBloom(b.encode())
	for i := 0; i < 1000; i++ {
		if !b2.mayContain([]byte(fmt.Sprintf("k%d", i))) {
			t.Fatalf("false negative after decode for k%d", i)
		}
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
