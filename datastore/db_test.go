package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	testSegmentSize    = 45 
	smallSegmentSize   = 35
	compactionWaitTime = 2 * time.Second
)

func TestDb_Put(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "datastore_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	database, err := createTestDatabase(tempDir, testSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	testPairs := []struct {
		key   string
		value string
	}{
		{"1", "v1"},
		{"2", "v2"},
		{"3", "v3"},
	}

	t.Run("put and get operations", func(t *testing.T) {
		for _, pair := range testPairs {
			err := database.Put(pair.key, pair.value)
			if err != nil {
				t.Errorf("Failed to put key %s: %v", pair.key, err)
			}

			time.Sleep(10 * time.Millisecond)

			retrievedValue, err := database.Get(pair.key)
			if err != nil {
				t.Errorf("Failed to get key %s: %v", pair.key, err)
			}

			if retrievedValue != pair.value {
				t.Errorf("Value mismatch for key %s: expected %s, got %s", pair.key, pair.value, retrievedValue)
			}
		}
	})

	t.Run("database recovery after restart", func(t *testing.T) {
		time.Sleep(100 * time.Millisecond)
		database.Close()

		recoveredDb, err := createTestDatabase(tempDir, 10)
		if err != nil {
			t.Fatal(err)
		}
		defer recoveredDb.Close()

		for _, pair := range testPairs {
			retrievedValue, err := recoveredDb.Get(pair.key)
			if err != nil {
				t.Errorf("Failed to get key %s after recovery: %v", pair.key, err)
			}

			if retrievedValue != pair.value {
				t.Errorf("Value mismatch after recovery for key %s: expected %s, got %s", pair.key, pair.value, retrievedValue)
			}
		}
	})
}

func TestDb_Segmentation(t *testing.T) {
	testDirectory, err := ioutil.TempDir("", "segmentation_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDirectory)

	database, err := createTestDatabase(testDirectory, smallSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	t.Run("segment creation on size limit", func(t *testing.T) {
		database.Put("1", "v1")
		time.Sleep(50 * time.Millisecond)

		database.Put("2", "v2")
		time.Sleep(50 * time.Millisecond)

		database.Put("3", "v3")
		time.Sleep(50 * time.Millisecond)

		database.Put("2", "v5")
		time.Sleep(50 * time.Millisecond)

		time.Sleep(200 * time.Millisecond)

		finalSegmentCount := len(database.segments)
		if finalSegmentCount < 2 {
			t.Errorf("Expected at least 2 segments due to size limit, got %d", finalSegmentCount)
		}
	})

	t.Run("compaction trigger and completion", func(t *testing.T) {
		database.Put("4", "v4")
		database.Put("5", "v5")
		database.Put("6", "v6")

		time.Sleep(200 * time.Millisecond)

		segmentCountBeforeCompaction := len(database.segments)
		if segmentCountBeforeCompaction >= 3 {
			time.Sleep(compactionWaitTime)

			segmentCountAfterCompaction := len(database.segments)
			if segmentCountAfterCompaction >= segmentCountBeforeCompaction {
				t.Errorf("Compaction should reduce segment count: before %d, after %d",
					segmentCountBeforeCompaction, segmentCountAfterCompaction)
			}
		}
	})

	t.Run("data integrity after compaction", func(t *testing.T) {
		retrievedValue, err := database.Get("2")
		if err != nil {
			t.Errorf("Failed to retrieve key after compaction: %v", err)
		}

		expectedValue := "v5"
		if retrievedValue != expectedValue {
			t.Errorf("Data corruption after compaction: expected %s, got %s", expectedValue, retrievedValue)
		}
	})

	t.Run("compacted segment is not empty and valid", func(t *testing.T) {
		compactedSegmentFile, err := os.Open(database.segments[0].path)
		if err != nil {
			t.Error(err)
			return
		}
		defer compactedSegmentFile.Close()

		fileInfo, err := compactedSegmentFile.Stat()
		if err != nil {
			t.Error(err)
			return
		}

		actualSize := fileInfo.Size()
		if actualSize == 0 {
			t.Errorf("Compacted segment file is empty, expected non-zero size")
		} else {
			t.Logf("Compacted segment file size: %d bytes", actualSize)
		}
	})
}

func createTestDatabase(directory string, segmentSize int64) (*Db, error) {
	return CreateDb(directory, segmentSize)
}