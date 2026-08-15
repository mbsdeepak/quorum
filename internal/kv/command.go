// Package kv turns the Raft log + LSM store into a replicated key-value store.
// Client writes become commands appended to the Raft log; once committed, every
// node applies them to its local LSM engine, so all replicas converge on the
// same data.
package kv

import "encoding/binary"

// Op is the mutation a command carries.
type Op uint8

const (
	OpPut Op = iota
	OpDelete
)

// Command is a single replicated mutation. It is the opaque []byte that Raft
// replicates; only this package knows how to interpret it.
type Command struct {
	Op    Op
	Key   []byte
	Value []byte
}

// Encode serializes a command:  | op u8 | keyLen u32 | key | valLen u32 | val |
func (c Command) Encode() []byte {
	out := make([]byte, 0, 1+4+len(c.Key)+4+len(c.Value))
	out = append(out, byte(c.Op))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(c.Key)))
	out = append(out, c.Key...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(c.Value)))
	out = append(out, c.Value...)
	return out
}

// DecodeCommand reverses Encode.
func DecodeCommand(b []byte) (Command, bool) {
	if len(b) < 1+4 {
		return Command{}, false
	}
	c := Command{Op: Op(b[0])}
	off := 1
	kl := int(binary.LittleEndian.Uint32(b[off : off+4]))
	off += 4
	if off+kl+4 > len(b) {
		return Command{}, false
	}
	c.Key = append([]byte(nil), b[off:off+kl]...)
	off += kl
	vl := int(binary.LittleEndian.Uint32(b[off : off+4]))
	off += 4
	if off+vl > len(b) {
		return Command{}, false
	}
	c.Value = append([]byte(nil), b[off:off+vl]...)
	return c, true
}
