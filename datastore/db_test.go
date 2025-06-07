package datastore

import (
	"os"
	"strings"
	"testing"
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
