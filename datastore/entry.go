package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type entry struct {
	key, value string
}

// 0           4     8     kl+8    kl+12     kl+12+vl    <-- offset
// (full size) (kl)  (key) (vl)    (value)   (hash[20])
// 4           4     ....  4       .....     20          <-- length

func (e *entry) Encode() []byte {
	kl, vl := len(e.key), len(e.value)
	hash := sha1.Sum([]byte(e.value)) // [20]byte

	size := kl + vl + 12 + len(hash)
	res := make([]byte, size)

	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	copy(res[kl+12+vl:], hash[:])

	return res
}

func (e *entry) Decode(input []byte) error {
	kl := int(binary.LittleEndian.Uint32(input[4:]))
	e.key = string(input[8 : 8+kl])

	vl := int(binary.LittleEndian.Uint32(input[8+kl:]))
	valueStart := 12 + kl
	e.value = string(input[valueStart : valueStart+vl])

	expectedHash := input[valueStart+vl:]
	actualHash := sha1.Sum([]byte(e.value))

	if !equalHash(expectedHash, actualHash[:]) {
		return fmt.Errorf("hash mismatch for key %s", e.key)
	}
	return nil
}

func equalHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (e *entry) DecodeFromReader(in *bufio.Reader) (int, error) {
	sizeBuf, err := in.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return 0, fmt.Errorf("DecodeFromReader, cannot read size: %w", err)
	}
	size := int(binary.LittleEndian.Uint32(sizeBuf))
	buf := make([]byte, size)
	n, err := in.Read(buf)
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader, cannot read record: %w", err)
	}
	if err := e.Decode(buf); err != nil {
		return n, err
	}
	return n, nil
}
