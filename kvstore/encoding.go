package kvstore

import (
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
)

// Commands and snapshots use a small explicit binary encoding rather than a
// general-purpose one.
//
// The reason is determinism. Everything here is fed to a replicated state
// machine, and a snapshot taken on two nodes with the same contents must produce
// the same bytes — otherwise identical replicas look different to anything that
// compares them. Encoders that range over a Go map, or that emit struct fields
// in reflection order, do not guarantee that. Sorting the keys and writing them
// by hand does.

const (
	maxKeyLen   = 1 << 20  // 1 MiB
	maxValueLen = 16 << 20 // 16 MiB
)

// EncodeCommand renders a command for the log.
func EncodeCommand(cmd Command) ([]byte, error) {
	if len(cmd.Key) == 0 {
		return nil, fmt.Errorf("kvstore: key must not be empty")
	}
	if len(cmd.Key) > maxKeyLen {
		return nil, fmt.Errorf("kvstore: key is %d bytes, limit is %d", len(cmd.Key), maxKeyLen)
	}
	if len(cmd.Value) > maxValueLen {
		return nil, fmt.Errorf("kvstore: value is %d bytes, limit is %d", len(cmd.Value), maxValueLen)
	}

	buf := make([]byte, 0, 1+binary.MaxVarintLen64*2+len(cmd.Key)+len(cmd.Value))
	buf = append(buf, byte(cmd.Op))
	buf = appendBytes(buf, cmd.Key)
	if cmd.Op == OpSet {
		buf = appendBytes(buf, cmd.Value)
	}
	return buf, nil
}

// DecodeCommand parses a command from a log entry.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) == 0 {
		return Command{}, fmt.Errorf("kvstore: empty command")
	}

	cmd := Command{Op: Op(b[0])}
	rest := b[1:]

	key, rest, err := readBytes(rest, "key")
	if err != nil {
		return Command{}, err
	}
	cmd.Key = key

	if cmd.Op == OpSet {
		value, _, err := readBytes(rest, "value")
		if err != nil {
			return Command{}, err
		}
		cmd.Value = value
	}
	return cmd, nil
}

// SetCommand is shorthand for the common case.
func SetCommand(key, value []byte) ([]byte, error) {
	return EncodeCommand(Command{Op: OpSet, Key: key, Value: value})
}

// DeleteCommand is shorthand for the other one.
func DeleteCommand(key []byte) ([]byte, error) {
	return EncodeCommand(Command{Op: OpDelete, Key: key})
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// encodeMap writes the map with its keys in sorted order, so that two nodes
// holding the same data produce byte-identical snapshots.
func encodeMap(m map[string][]byte) []byte {
	keys := slices.Sorted(maps.Keys(m))

	buf := make([]byte, 0, 8)
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendBytes(buf, []byte(k))
		buf = appendBytes(buf, m[k])
	}
	return buf
}

func decodeMap(b []byte) (map[string][]byte, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, fmt.Errorf("kvstore: truncated snapshot header")
	}
	rest := b[n:]

	m := make(map[string][]byte, count)
	for i := uint64(0); i < count; i++ {
		key, r, err := readBytes(rest, "snapshot key")
		if err != nil {
			return nil, err
		}
		value, r2, err := readBytes(r, "snapshot value")
		if err != nil {
			return nil, err
		}
		m[string(key)] = value
		rest = r2
	}
	return m, nil
}

// ---------------------------------------------------------------------------

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func readBytes(b []byte, what string) (value, rest []byte, err error) {
	n, read := binary.Uvarint(b)
	if read <= 0 {
		return nil, nil, fmt.Errorf("kvstore: truncated length reading %s", what)
	}
	b = b[read:]
	if uint64(len(b)) < n {
		return nil, nil, fmt.Errorf("kvstore: %s claims %d bytes, %d remain", what, n, len(b))
	}
	return b[:n], b[n:], nil
}
