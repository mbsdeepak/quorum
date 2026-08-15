package lsm

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// bloomFilter is a classic Bloom filter used to skip SSTables that provably do
// not contain a key, which avoids a wasted disk read on the common negative
// lookup. The k bit positions are derived from two base hashes via double
// hashing (Kirsch–Mitzenmacher), so we pay for two real hashes, not k.
type bloomFilter struct {
	bits []byte
	k    uint32 // number of hash probes
	m    uint32 // number of bits
}

// newBloom sizes a filter for n keys at the target false-positive rate.
func newBloom(n int, fpRate float64) *bloomFilter {
	if n < 1 {
		n = 1
	}
	// m = -n·ln(p) / (ln2)^2 ;  k = (m/n)·ln2
	m := uint32(math.Ceil(-float64(n) * math.Log(fpRate) / (math.Ln2 * math.Ln2)))
	if m < 8 {
		m = 8
	}
	k := uint32(math.Round(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	return &bloomFilter{bits: make([]byte, (m+7)/8), k: k, m: m}
}

func (b *bloomFilter) baseHashes(key []byte) (uint32, uint32) {
	h1 := fnv.New32a()
	h1.Write(key)
	h2 := fnv.New32()
	h2.Write(key)
	return h1.Sum32(), h2.Sum32()
}

func (b *bloomFilter) add(key []byte) {
	h1, h2 := b.baseHashes(key)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + i*h2) % b.m
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (b *bloomFilter) mayContain(key []byte) bool {
	h1, h2 := b.baseHashes(key)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + i*h2) % b.m
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// encode serializes the filter for storage alongside its SSTable.
func (b *bloomFilter) encode() []byte {
	out := make([]byte, 8+len(b.bits))
	binary.LittleEndian.PutUint32(out[0:4], b.k)
	binary.LittleEndian.PutUint32(out[4:8], b.m)
	copy(out[8:], b.bits)
	return out
}

func decodeBloom(buf []byte) *bloomFilter {
	return &bloomFilter{
		k:    binary.LittleEndian.Uint32(buf[0:4]),
		m:    binary.LittleEndian.Uint32(buf[4:8]),
		bits: append([]byte(nil), buf[8:]...),
	}
}
