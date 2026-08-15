package lsm

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestSSTableRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "t.dat")
	var entries []entry
	for i := 0; i < 100; i++ {
		entries = append(entries, entry{
			key:  []byte(fmt.Sprintf("k%03d", i)),
			val:  []byte(fmt.Sprintf("v%d", i)),
			kind: kindPut,
		})
	}
	entries[50].kind = kindDelete // one tombstone to read back
	if err := writeSSTable(p, entries); err != nil {
		t.Fatal(err)
	}
	r, err := openSSTable(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()

	for i := 0; i < 100; i++ {
		v, kind, ok, err := r.get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil || !ok {
			t.Fatalf("get k%03d: ok=%v err=%v", i, ok, err)
		}
		wantKind := kindPut
		if i == 50 {
			wantKind = kindDelete
		}
		if kind != wantKind {
			t.Fatalf("k%03d kind=%d want %d", i, kind, wantKind)
		}
		if string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%03d val=%q", i, v)
		}
	}
	// Keys outside the stored range and in-range-but-absent must miss.
	if _, _, ok, _ := r.get([]byte("zzz")); ok {
		t.Fatal("expected miss for key after range")
	}
	if _, _, ok, _ := r.get([]byte("aaa")); ok {
		t.Fatal("expected miss for key before range")
	}
	if _, _, ok, _ := r.get([]byte("k0505")); ok {
		t.Fatal("expected miss for absent in-range key")
	}
}
