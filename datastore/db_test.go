package datastore

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			_ = db.Put(pair[0], pair[1])
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("File did not grow: before %d, after %d", sizeBefore, sizeAfter)
		}
	})

	t.Run("reopen db", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp)
		if err != nil {
			t.Fatal(err)
		}
		unique := map[string]string{}
		for _, p := range pairs {
			unique[p[0]] = p[1]
		}
		for k, v := range unique {
			got, err := db.Get(k)
			if err != nil {
				t.Errorf("Get failed for %s: %s", k, err)
			}
			if got != v {
				t.Errorf("Expected %s, got %s", v, got)
			}
		}
	})
}

func TestDb_Segments(t *testing.T) {
	tmp := t.TempDir()

	segmentLimit := int64(100) // байтів

	db, err := OpenWithLimit(tmp, segmentLimit)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	for i := 0; i < 20; i++ {
		key := "key" + string(rune(i+'a'))
		value := strings.Repeat("v", 20)
		err := db.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	count := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "segment-") {
			count++
		}
	}
	if count < 2 {
		t.Errorf("Expected multiple segment files, found %d", count)
	}

	for i := 0; i < 20; i++ {
		key := "key" + string(rune(i+'a'))
		val, err := db.Get(key)
		if err != nil {
			t.Errorf("Get failed for key=%s: %v", key, err)
		}
		if val != strings.Repeat("v", 20) {
			t.Errorf("Incorrect value for %s: %s", key, val)
		}
	}
}
func TestDb_ParallelPutGet(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	const count = 100
	keys := make([]string, count)
	values := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[i] = fmt.Sprintf("value-%d", i)
	}

	var wg sync.WaitGroup

	// Паралельно записуємо
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := db.Put(keys[i], values[i]); err != nil {
				t.Errorf("Put failed for key %s: %v", keys[i], err)
			}
		}(i)
	}

	wg.Wait()

	// Невелика затримка, щоб ensure запис у writeLoop завершився
	time.Sleep(100 * time.Millisecond)

	// Паралельно читаємо
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			val, err := db.Get(keys[i])
			if err != nil {
				t.Errorf("Get failed for key %s: %v", keys[i], err)
				return
			}
			if val != values[i] {
				t.Errorf("Wrong value for key %s: got %s, expected %s", keys[i], val, values[i])
			}
		}(i)
	}

	wg.Wait()
}
func TestDb_DetectsCorruptedValue(t *testing.T) {
	tmp := t.TempDir()

	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	key := "sensitive"
	value := "secret-data"
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	db.mu.RLock()
	ref, ok := db.index[key]
	db.mu.RUnlock()
	if !ok {
		t.Fatal("key not found in index")
	}

	path := fmt.Sprintf("%s/segment-%d", tmp, ref.segmentId)
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("cannot open segment file: %v", err)
	}
	defer file.Close()

	seekOffset := ref.offset + int64(12+len(key)+len(value))
	if _, err := file.Seek(seekOffset, 0); err != nil {
		t.Fatalf("seek failed: %v", err)
	}

	if _, err := file.Write([]byte{0x00}); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	_, err = db.Get(key)
	if err == nil {
		t.Fatal("expected error when reading corrupted data, got nil")
	}
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("expected ErrCorrupted, got: %v", err)
	}
}
