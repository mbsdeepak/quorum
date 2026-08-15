package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"sort"
)

// entry is a single key/value record with its kind (put or tombstone). It is
// the common currency between the memtable, the SSTable, and compaction.
type entry struct {
	key  []byte
	val  []byte
	kind recordKind
}

const (
	// sstMagic tags the footer so we can detect a truncated or foreign file.
	sstMagic uint64 = 0x5153535442303031 // "QSSTB001"
	// indexInterval is how many entries sit between two sparse-index anchors.
	indexInterval = 16
)

// An SSTable (Sorted String Table) is an immutable on-disk file. Layout:
//
//	[ data:  entry* sorted by key ]
//	[ bloom filter block          ]
//	[ sparse index block          ]
//	[ footer (40 bytes)           ]
//
// On-disk entry:  | keyLen u32 | key | kind u8 | valLen u32 | val |
// Footer:         | bloomOff u64 | bloomLen u64 | idxOff u64 | idxLen u64 | magic u64 |
//
// The bloom filter and sparse index are small enough to hold in memory; data
// entries are read from disk on demand. The index anchors one key every
// indexInterval entries, so a lookup binary-searches the anchors and then scans
// a single short run — no full-file scan, no per-key in-memory index.

// writeSSTable builds an SSTable at path from entries, which MUST be sorted by
// key ascending with unique keys.
func writeSSTable(path string, entries []entry) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(path)
		}
	}()
	bw := bufio.NewWriter(f)

	bloom := newBloom(len(entries), 0.01)
	type anchor struct {
		key    []byte
		offset uint64
	}
	var index []anchor
	var offset uint64

	buf := make([]byte, 0, 64)
	for i, e := range entries {
		bloom.add(e.key)
		if i%indexInterval == 0 {
			index = append(index, anchor{key: e.key, offset: offset})
		}
		buf = buf[:0]
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.key)))
		buf = append(buf, e.key...)
		buf = append(buf, byte(e.kind))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.val)))
		buf = append(buf, e.val...)
		if _, err = bw.Write(buf); err != nil {
			return err
		}
		offset += uint64(len(buf))
	}

	// Bloom block.
	bloomOffset := offset
	bloomBytes := bloom.encode()
	if _, err = bw.Write(bloomBytes); err != nil {
		return err
	}
	offset += uint64(len(bloomBytes))

	// Sparse index block: | count u32 | (keyLen u32, key, offset u64)* |
	indexOffset := offset
	ibuf := make([]byte, 0, 1024)
	ibuf = binary.LittleEndian.AppendUint32(ibuf, uint32(len(index)))
	for _, a := range index {
		ibuf = binary.LittleEndian.AppendUint32(ibuf, uint32(len(a.key)))
		ibuf = append(ibuf, a.key...)
		ibuf = binary.LittleEndian.AppendUint64(ibuf, a.offset)
	}
	if _, err = bw.Write(ibuf); err != nil {
		return err
	}

	// Footer.
	var footer [40]byte
	binary.LittleEndian.PutUint64(footer[0:8], bloomOffset)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(bloomBytes)))
	binary.LittleEndian.PutUint64(footer[16:24], indexOffset)
	binary.LittleEndian.PutUint64(footer[24:32], uint64(len(ibuf)))
	binary.LittleEndian.PutUint64(footer[32:40], sstMagic)
	if _, err = bw.Write(footer[:]); err != nil {
		return err
	}

	if err = bw.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// sstReader is an opened, immutable SSTable.
type sstReader struct {
	f       *os.File
	bloom   *bloomFilter
	index   []indexEntry
	dataEnd uint64 // entries live in [0, dataEnd); == bloom block offset
}

type indexEntry struct {
	key    []byte
	offset uint64
}

func openSSTable(path string) (*sstReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < 40 {
		f.Close()
		return nil, errCorruptSST
	}
	var footer [40]byte
	if _, err := f.ReadAt(footer[:], fi.Size()-40); err != nil {
		f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint64(footer[32:40]) != sstMagic {
		f.Close()
		return nil, errCorruptSST
	}
	bloomOffset := binary.LittleEndian.Uint64(footer[0:8])
	bloomLen := binary.LittleEndian.Uint64(footer[8:16])
	indexOffset := binary.LittleEndian.Uint64(footer[16:24])
	indexLen := binary.LittleEndian.Uint64(footer[24:32])

	bloomBuf := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBuf, int64(bloomOffset)); err != nil {
		f.Close()
		return nil, err
	}
	indexBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBuf, int64(indexOffset)); err != nil {
		f.Close()
		return nil, err
	}
	index, err := decodeIndex(indexBuf)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &sstReader{f: f, bloom: decodeBloom(bloomBuf), index: index, dataEnd: bloomOffset}, nil
}

func decodeIndex(buf []byte) ([]indexEntry, error) {
	if len(buf) < 4 {
		return nil, errCorruptSST
	}
	n := binary.LittleEndian.Uint32(buf[0:4])
	off := 4
	out := make([]indexEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		if off+4 > len(buf) {
			return nil, errCorruptSST
		}
		kl := int(binary.LittleEndian.Uint32(buf[off : off+4]))
		off += 4
		if off+kl+8 > len(buf) {
			return nil, errCorruptSST
		}
		key := append([]byte(nil), buf[off:off+kl]...)
		off += kl
		offset := binary.LittleEndian.Uint64(buf[off : off+8])
		off += 8
		out = append(out, indexEntry{key: key, offset: offset})
	}
	return out, nil
}

// get looks up key in this table. Returns (val, kind, found, err). found=false
// means the key is not in this table (bloom miss, or absent from its block).
func (r *sstReader) get(key []byte) ([]byte, recordKind, bool, error) {
	if !r.bloom.mayContain(key) {
		return nil, 0, false, nil
	}
	// Find the last anchor whose key <= target: the block that could hold it.
	hi := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].key, key) > 0
	})
	if hi == 0 {
		return nil, 0, false, nil // key precedes the first stored key
	}
	start := r.index[hi-1].offset
	end := r.dataEnd
	if hi < len(r.index) {
		end = r.index[hi].offset
	}

	buf := make([]byte, end-start)
	if _, err := r.f.ReadAt(buf, int64(start)); err != nil {
		return nil, 0, false, err
	}
	for off := 0; off < len(buf); {
		kl := int(binary.LittleEndian.Uint32(buf[off : off+4]))
		off += 4
		k := buf[off : off+kl]
		off += kl
		kind := recordKind(buf[off])
		off++
		vl := int(binary.LittleEndian.Uint32(buf[off : off+4]))
		off += 4
		v := buf[off : off+vl]
		off += vl
		switch bytes.Compare(k, key) {
		case 0:
			return append([]byte(nil), v...), kind, true, nil
		case 1:
			return nil, 0, false, nil // sorted: passed where key would be
		}
	}
	return nil, 0, false, nil
}

// all reads every entry in the table in key order (used by compaction).
func (r *sstReader) all() ([]entry, error) {
	buf := make([]byte, r.dataEnd)
	if _, err := r.f.ReadAt(buf, 0); err != nil && r.dataEnd > 0 {
		return nil, err
	}
	var out []entry
	for off := 0; off < len(buf); {
		kl := int(binary.LittleEndian.Uint32(buf[off : off+4]))
		off += 4
		k := append([]byte(nil), buf[off:off+kl]...)
		off += kl
		kind := recordKind(buf[off])
		off++
		vl := int(binary.LittleEndian.Uint32(buf[off : off+4]))
		off += 4
		v := append([]byte(nil), buf[off:off+vl]...)
		off += vl
		out = append(out, entry{key: k, val: v, kind: kind})
	}
	return out, nil
}

func (r *sstReader) close() error { return r.f.Close() }
