package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	outFileNamePrefix     = "segment-"
	defaultMaxSegmentSize = int64(10 * 1024 * 1024) // 10 MB
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	dir              string
	segmentLimit     int64
	currentSegment   *os.File
	currentSegmentId int
	currentOffset    int64
	index            map[string]segmentRef
}

type segmentRef struct {
	segmentId int
	offset    int64
}

func Open(dir string) (*Db, error) {
	return OpenWithLimit(dir, defaultMaxSegmentSize)
}

func OpenWithLimit(dir string, segmentLimit int64) (*Db, error) {
	db := &Db{
		dir:          dir,
		segmentLimit: segmentLimit,
		index:        make(map[string]segmentRef),
	}

	err := db.loadSegments()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Db) loadSegments() error {
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	segmentIds := []int{}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), outFileNamePrefix) {
			idStr := strings.TrimPrefix(file.Name(), outFileNamePrefix)
			id, err := strconv.Atoi(idStr)
			if err == nil {
				segmentIds = append(segmentIds, id)
			}
		}
	}

	if len(segmentIds) == 0 {
		return db.createNewSegment()
	}

	maxId := -1
	for _, id := range segmentIds {
		if id > maxId {
			maxId = id
		}
		err := db.recoverSegment(id)
		if err != nil {
			return err
		}
	}

	db.currentSegmentId = maxId
	segFile, err := os.OpenFile(filepath.Join(db.dir, segmentFilename(maxId)), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	db.currentSegment = segFile
	info, err := segFile.Stat()
	if err != nil {
		return err
	}
	db.currentOffset = info.Size()

	return nil
}

func (db *Db) recoverSegment(id int) error {
	path := filepath.Join(db.dir, segmentFilename(id))
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	offset := int64(0)
	for {
		var record entry
		n, err := record.DecodeFromReader(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("corrupted segment: %w", err)
		}
		db.index[record.key] = segmentRef{
			segmentId: id,
			offset:    offset,
		}
		offset += int64(n)
	}
	return nil
}

func (db *Db) createNewSegment() error {
	db.currentSegmentId++
	name := segmentFilename(db.currentSegmentId)
	path := filepath.Join(db.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	db.currentSegment = f
	db.currentOffset = 0
	return nil
}

func (db *Db) Close() error {
	if db.currentSegment != nil {
		return db.currentSegment.Close()
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
	ref, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}
	path := filepath.Join(db.dir, segmentFilename(ref.segmentId))
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = f.Seek(ref.offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err := record.DecodeFromReader(bufio.NewReader(f)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{key: key, value: value}
	data := e.Encode()

	if db.currentOffset+int64(len(data)) > db.segmentLimit {
		if err := db.currentSegment.Close(); err != nil {
			return err
		}
		if err := db.createNewSegment(); err != nil {
			return err
		}
	}

	n, err := db.currentSegment.Write(data)
	if err != nil {
		return err
	}

	db.index[key] = segmentRef{
		segmentId: db.currentSegmentId,
		offset:    db.currentOffset,
	}
	db.currentOffset += int64(n)
	return nil
}

func (db *Db) Size() (int64, error) {
	return db.currentOffset, nil
}

func segmentFilename(id int) string {
	return fmt.Sprintf("%s%d", outFileNamePrefix, id)
}
