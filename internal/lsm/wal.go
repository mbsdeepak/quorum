package lsm

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// recordKind distinguishes a value write from a deletion (tombstone).
type recordKind uint8

const (
	kindPut recordKind = iota
	kindDelete
)

// WAL is an append-only write-ahead log. Every mutation is durably appended
// here before it touches the in-memory memtable, so an un-flushed memtable can
// be rebuilt after a crash by replaying the log.
//
// Record framing (all little-endian):
//
//	| crc32 (4) | length (4) | kind (1) | keyLen (4) | key | valLen (4) | val |
//
// The crc32 covers everything after the length field.
type WAL struct {
	f  *os.File
	bw *bufio.Writer
}

// OpenWAL opens (or creates) the log at path for appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, bw: bufio.NewWriter(f)}, nil
}

// Append writes one record. It is buffered; call Sync to make it durable.
func (w *WAL) Append(kind recordKind, key, val []byte) error {
	body := make([]byte, 0, 1+4+len(key)+4+len(val))
	body = append(body, byte(kind))
	body = binary.LittleEndian.AppendUint32(body, uint32(len(key)))
	body = append(body, key...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(val)))
	body = append(body, val...)

	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], crc32.ChecksumIEEE(body))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(body)))
	if _, err := w.bw.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.bw.Write(body)
	return err
}

// Sync flushes buffered data and fsyncs it to disk. Callers choose the
// durability cadence (per-write vs. batched) by when they call this.
func (w *WAL) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close flushes and closes the underlying file.
func (w *WAL) Close() error {
	if err := w.bw.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// walEntry is one decoded record.
type walEntry struct {
	kind recordKind
	key  []byte
	val  []byte
}

// ReplayWAL reads every intact record from path in order. A trailing partial or
// corrupt record (a crash mid-write) stops replay cleanly rather than erroring —
// everything written before it is still valid and is delivered to fn.
func ReplayWAL(path string, fn func(walEntry) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end, or a torn header at the tail
			}
			return err
		}
		sum := binary.LittleEndian.Uint32(hdr[0:4])
		n := binary.LittleEndian.Uint32(hdr[4:8])
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil // torn body at the tail
		}
		if crc32.ChecksumIEEE(body) != sum {
			return nil // corrupt tail
		}
		e, err := decodeWALBody(body)
		if err != nil {
			return nil
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}

func decodeWALBody(body []byte) (walEntry, error) {
	if len(body) < 1+4 {
		return walEntry{}, errShortRecord
	}
	kind := recordKind(body[0])
	off := 1
	keyLen := int(binary.LittleEndian.Uint32(body[off : off+4]))
	off += 4
	if off+keyLen+4 > len(body) {
		return walEntry{}, errShortRecord
	}
	key := body[off : off+keyLen]
	off += keyLen
	valLen := int(binary.LittleEndian.Uint32(body[off : off+4]))
	off += 4
	if off+valLen > len(body) {
		return walEntry{}, errShortRecord
	}
	val := body[off : off+valLen]
	return walEntry{
		kind: kind,
		key:  append([]byte(nil), key...),
		val:  append([]byte(nil), val...),
	}, nil
}
