package codec

import (
	"encoding/binary"
	"fmt"
)

func appendUvarint(b []byte, v uint64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], v)
	return append(b, scratch[:n]...)
}

func readUvarint(b []byte, field string) (uint64, int, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, fmt.Errorf("codec: truncated hard state reading %s", field)
	}
	return v, n, nil
}
