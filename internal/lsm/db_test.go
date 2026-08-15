package lsm

import (
	"fmt"
	"testing"
)

func openTestDB(t *testing.T, opts Options) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db, dir
}

func TestPutGetOverwriteDelete(t *testing.T) {
	db, _ := openTestDB(t, DefaultOptions())
	defer db.Close()

	must(t, db.Put([]byte("a"), []byte("1")))
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "1" {
		t.Fatalf("get a = %q, %v", got, err)
	}

	must(t, db.Put([]byte("a"), []byte("2")))
	if got, _ := db.Get([]byte("a")); string(got) != "2" {
		t.Fatalf("overwrite: got %q", got)
	}

	must(t, db.Delete([]byte("a")))
	if _, err := db.Get([]byte("a")); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if _, err := db.Get([]byte("missing")); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for missing, got %v", err)
	}
}

func TestFlushThenRead(t *testing.T) {
	db, _ := openTestDB(t, DefaultOptions())
	defer db.Close()

	for i := 0; i < 200; i++ {
		must(t, db.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%d", i))))
	}
	must(t, db.Flush()) // force data onto disk as an SSTable

	for i := 0; i < 200; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("key-%04d", i)))
		if err != nil {
			t.Fatalf("get key-%04d: %v", i, err)
		}
		if want := fmt.Sprintf("val-%d", i); string(got) != want {
			t.Fatalf("key-%04d = %q want %q", i, got, want)
		}
	}
}

// TestRecoveryFromWAL simulates a crash: writes are made but never flushed, the
// DB is closed, and on reopen the data must return via WAL replay.
func TestRecoveryFromWAL(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		must(t, db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i))))
	}
	must(t, db.Close()) // no Flush: memtable exists only in the WAL

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 50; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("k%02d", i)))
		if err != nil {
			t.Fatalf("recover k%02d: %v", i, err)
		}
		if want := fmt.Sprintf("v%d", i); string(got) != want {
			t.Fatalf("recover k%02d = %q want %q", i, got, want)
		}
	}
}

// TestCompaction drives many flushes with a tiny memtable so compaction runs,
// then checks that overwritten values win and deleted keys stay gone.
func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableThreshold: 256, CompactionTrigger: 3, SyncWrites: false}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 500; i++ {
		must(t, db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%d", i))))
	}
	for i := 0; i < 100; i++ {
		must(t, db.Delete([]byte(fmt.Sprintf("k%05d", i))))
	}
	must(t, db.Flush())

	for i := 0; i < 100; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("k%05d", i))); err != ErrNotFound {
			t.Fatalf("k%05d should be deleted, got %v", i, err)
		}
	}
	for i := 100; i < 500; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k%05d", i)))
		if err != nil {
			t.Fatalf("k%05d: %v", i, err)
		}
		if want := fmt.Sprintf("v%d", i); string(got) != want {
			t.Fatalf("k%05d = %q want %q", i, got, want)
		}
	}
}

// TestReopenAfterCompaction ensures persisted SSTables reload correctly.
func TestReopenAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableThreshold: 256, CompactionTrigger: 3, SyncWrites: true}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		must(t, db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%d", i))))
	}
	must(t, db.Flush())
	must(t, db.Close())

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 300; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("k%05d", i)))
		if err != nil {
			t.Fatalf("reopen k%05d: %v", i, err)
		}
		if want := fmt.Sprintf("v%d", i); string(got) != want {
			t.Fatalf("reopen k%05d = %q want %q", i, got, want)
		}
	}
}
