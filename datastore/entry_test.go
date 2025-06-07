package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{"key", "value"}
	encoded := e.Encode()

	var decoded entry
	err := decoded.Decode(encoded)
	if err != nil {
		t.Fatal("Decode failed:", err)
	}
	if decoded.key != "key" {
		t.Error("incorrect key")
	}
	if decoded.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	var (
		a = entry{"key", "test-value"}
		b entry
	)

	originalBytes := a.Encode()

	err := b.Decode(originalBytes)
	if err != nil {
		t.Fatal("decode failed:", err)
	}
	t.Log("encode/decode", a, b)
	if a != b {
		t.Error("Encode/Decode mismatch")
	}

	b = entry{}
	n, err := b.DecodeFromReader(bufio.NewReader(bytes.NewReader(originalBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("encode/decodeFromReader", a, b)
	if a != b {
		t.Error("Encode/DecodeFromReader mismatch")
	}
	if n != len(originalBytes) {
		t.Errorf("DecodeFromReader() read %d bytes, expected %d", n, len(originalBytes))
	}
}

func TestEntry_HashMismatch(t *testing.T) {
	e := entry{"abc", "correct"}
	encoded := e.Encode()

	encoded[len(encoded)-1] ^= 0xFF

	var corrupted entry
	err := corrupted.Decode(encoded)
	if err == nil {
		t.Fatal("expected hash mismatch error, got nil")
	}
	t.Logf("Got expected error: %v", err)
}
