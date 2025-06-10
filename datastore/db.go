package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	dataFileName    = "current-data"
	bufferSize      = 8192
	defaultFileMode = 0644
	minSegments     = 3
)

type keyIndex map[string]int64

type Db struct {
	activeFile     *os.File
	activeFilePath string
	currentOffset  int64
	directory      string
	maxSegmentSize int64
	segmentCounter int
	segments       []*Segment
	fileLock       sync.Mutex
	segmentLock    sync.RWMutex
}

type Segment struct {
	startOffset int64
	keyIndex    keyIndex
	path        string
	mu          sync.RWMutex
}

func CreateDb(directory string, maxSegmentSize int64) (*Db, error) {
	if err := os.MkdirAll(directory, defaultFileMode); err != nil {
		return nil, err
	}

	database := &Db{
		segments:       make([]*Segment, 0),
		directory:      directory,
		maxSegmentSize: maxSegmentSize,
	}


	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() || !file.Type().IsRegular() || !filepath.HasPrefix(file.Name(), dataFileName) {
			continue
		}
		path := filepath.Join(directory, file.Name())
		segment := &Segment{
			path:     path,
			keyIndex: make(keyIndex),
		}
		database.segments = append(database.segments, segment)
	}

	if err := database.recoverAllSegments(); err != nil && err != io.EOF {
		return nil, err
	}

	if err := database.initializeNewSegment(); err != nil {
		return nil, err
	}

	return database, nil
}

func (db *Db) Close() error {
	if db.activeFile != nil {
		return db.activeFile.Close()
	}
	return nil
}

func (db *Db) initializeNewSegment() error {
	newFilePath := db.generateFileName()
	file, err := os.OpenFile(newFilePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, defaultFileMode)
	if err != nil {
		return err
	}

	segment := &Segment{
		path:     newFilePath,
		keyIndex: make(keyIndex),
	}

	if db.activeFile != nil {
		db.activeFile.Close()
	}

	db.activeFile = file
	db.currentOffset = 0
	db.activeFilePath = newFilePath

	db.segmentLock.Lock()
	db.segments = append(db.segments, segment)
	db.segmentLock.Unlock()


	if len(db.segments) >= minSegments {
		go db.compactOldSegments()
	}

	return nil
}

func (db *Db) generateFileName() string {
	fileName := filepath.Join(db.directory, fmt.Sprintf("%s%d", dataFileName, db.segmentCounter))
	db.segmentCounter++
	return fileName
}

func (db *Db) compactOldSegments() {
	db.segmentLock.Lock()
	defer db.segmentLock.Unlock()

	if len(db.segments) < minSegments {
		return
	}

	compactedFilePath := db.generateFileName()
	compactedFile, err := os.OpenFile(compactedFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, defaultFileMode)
	if err != nil {
		return
	}
	defer compactedFile.Close()

	compactedSegment := &Segment{
		path:     compactedFilePath,
		keyIndex: make(keyIndex),
	}

	var writeOffset int64
	keysWritten := make(map[string]bool)


	for i := len(db.segments) - 2; i >= 0; i-- {
		segment := db.segments[i]
		segment.mu.RLock()

		for key, position := range segment.keyIndex {
			if !keysWritten[key] {
				value, err := segment.readFromSegment(position)
				if err != nil {
					continue
				}

				record := entry{
					key:   key,
					value: value,
				}

				bytesWritten, err := compactedFile.Write(record.Encode())
				if err == nil {
					compactedSegment.keyIndex[key] = writeOffset
					writeOffset += int64(bytesWritten)
					keysWritten[key] = true
				}
			}
		}
		segment.mu.RUnlock()
	}

	
	newSegments := []*Segment{compactedSegment, db.segments[len(db.segments)-1]}
	for i := 0; i < len(db.segments)-1; i++ {
		_ = os.Remove(db.segments[i].path)
	}

	db.segments = newSegments
}

func (db *Db) recoverAllSegments() error {
	db.segmentLock.RLock()
	defer db.segmentLock.RUnlock()

	for _, segment := range db.segments {
		if err := db.recoverSegmentData(segment); err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

func (db *Db) recoverSegmentData(segment *Segment) error {
	file, err := os.Open(segment.path)
	if err != nil {
		return err
	}
	defer file.Close()

	return db.processRecovery(file, segment)
}

func (db *Db) processRecovery(file *os.File, segment *Segment) error {
	var err error
	var buffer [bufferSize]byte
	var currentOffset int64

	reader := bufio.NewReaderSize(file, bufferSize)
	for err == nil {
		var header, data []byte
		var bytesRead int

		header, err = reader.Peek(bufferSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}

		if len(header) < 4 {
			return io.EOF
		}

		recordSize := binary.LittleEndian.Uint32(header)
		if recordSize == 0 || recordSize > uint32(bufferSize*10) {
			return fmt.Errorf("invalid record size: %d", recordSize)
		}

		if recordSize < bufferSize {
			data = buffer[:recordSize]
		} else {
			data = make([]byte, recordSize)
		}

		bytesRead, err = reader.Read(data)
		if err == nil {
			if bytesRead != int(recordSize) {
				return fmt.Errorf("data corruption detected: expected %d bytes, got %d", recordSize, bytesRead)
			}

			var record entry
			record.Decode(data)

			segment.mu.Lock()
			segment.keyIndex[record.key] = currentOffset
			segment.mu.Unlock()

			currentOffset += int64(bytesRead)
		}
	}

	if segment == db.getCurrentSegment() {
		db.currentOffset = currentOffset
	}

	return err
}

func (db *Db) findKeyLocation(key string) (*Segment, int64, error) {
	db.segmentLock.RLock()
	defer db.segmentLock.RUnlock()

	for i := len(db.segments) - 1; i >= 0; i-- {
		segment := db.segments[i]
		segment.mu.RLock()
		position, found := segment.keyIndex[key]
		segment.mu.RUnlock()

		if found {
			return segment, position, nil
		}
	}
	return nil, 0, fmt.Errorf("key not found in datastore")
}

func (db *Db) Get(key string) (string, error) {
	segment, position, err := db.findKeyLocation(key)
	if err != nil {
		return "", err
	}

	value, err := segment.readFromSegment(position)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	db.fileLock.Lock()
	defer db.fileLock.Unlock()

	entryData := entry{key: key, value: value}
	entrySize := entryData.GetLength()


	fileInfo, err := db.activeFile.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+entrySize > db.maxSegmentSize {
		if err := db.initializeNewSegment(); err != nil {
			return err
		}
	}

	currentPos := db.currentOffset
	bytesWritten, err := db.activeFile.Write(entryData.Encode())
	if err != nil {
		return err
	}

	db.currentOffset += int64(bytesWritten)


	currentSegment := db.getCurrentSegment()
	currentSegment.mu.Lock()
	currentSegment.keyIndex[key] = currentPos
	currentSegment.mu.Unlock()

	return nil
}

func (db *Db) getCurrentSegment() *Segment {
	db.segmentLock.RLock()
	defer db.segmentLock.RUnlock()

	if len(db.segments) == 0 {
		return nil
	}
	return db.segments[len(db.segments)-1]
}

func (segment *Segment) readFromSegment(position int64) (string, error) {
	file, err := os.Open(segment.path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}
