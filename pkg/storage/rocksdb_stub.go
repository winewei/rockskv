//go:build !cgo || nocgo
// +build !cgo nocgo

package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// RocksDB stub implementation for environments without cgo/RocksDB
// This implementation persists data to a JSON file for local development

type RocksDB struct {
	config   *RocksDBConfig
	data     map[string][]byte
	mu       sync.RWMutex
	fileMu   sync.Mutex // Mutex for file operations
	dataFile string
	saveSeq  uint64 // Sequence number for unique temp file names
}

func NewRocksDB(config *RocksDBConfig) (*RocksDB, error) {
	if config == nil {
		config = DefaultRocksDBConfig()
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	dataFile := filepath.Join(config.DataDir, "data.json")

	r := &RocksDB{
		config:   config,
		data:     make(map[string][]byte),
		dataFile: dataFile,
	}

	// Load existing data from file
	if err := r.load(); err != nil {
		// Ignore error if file doesn't exist
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load data: %w", err)
		}
	}

	return r, nil
}

// load reads data from the JSON file
func (r *RocksDB) load() error {
	data, err := os.ReadFile(r.dataFile)
	if err != nil {
		return err
	}

	// Decode JSON - using map[string]string for JSON compatibility
	var strData map[string]string
	if err := json.Unmarshal(data, &strData); err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.data = make(map[string][]byte, len(strData))
	for k, v := range strData {
		r.data[k] = []byte(v)
	}

	return nil
}

// save writes data to the JSON file
func (r *RocksDB) save() error {
	r.mu.RLock()
	strData := make(map[string]string, len(r.data))
	for k, v := range r.data {
		strData[k] = string(v)
	}
	r.mu.RUnlock()

	data, err := json.Marshal(strData)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Use file mutex to serialize file operations
	r.fileMu.Lock()
	defer r.fileMu.Unlock()

	// Use a unique temp file name with atomic sequence number
	seq := atomic.AddUint64(&r.saveSeq, 1)
	tmpFile := fmt.Sprintf("%s.tmp.%d.%d", r.dataFile, os.Getpid(), seq)
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := os.Rename(tmpFile, r.dataFile); err != nil {
		os.Remove(tmpFile) // Clean up temp file on error
		return fmt.Errorf("failed to rename data file: %w", err)
	}

	return nil
}

func (r *RocksDB) Get(key []byte) ([]byte, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result, true, nil
}

func (r *RocksDB) Put(key, value []byte) error {
	r.mu.Lock()
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	r.data[string(key)] = valueCopy
	r.mu.Unlock()

	return r.save()
}

func (r *RocksDB) Delete(key []byte) error {
	r.mu.Lock()
	delete(r.data, string(key))
	r.mu.Unlock()

	return r.save()
}

func (r *RocksDB) BatchPut(items []KeyValueItem) error {
	r.mu.Lock()
	for _, item := range items {
		valueCopy := make([]byte, len(item.Value))
		copy(valueCopy, item.Value)
		r.data[string(item.Key)] = valueCopy
	}
	r.mu.Unlock()

	return r.save()
}

type stubSnapshot struct{}

func (r *RocksDB) CreateSnapshot() *stubSnapshot {
	return &stubSnapshot{}
}

func (r *RocksDB) ReleaseSnapshot(snapshot *stubSnapshot) {}

type stubIterator struct {
	keys   []string
	values [][]byte
	pos    int
	valid  bool
}

func (r *RocksDB) NewIterator() *stubIterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keys := make([]string, 0, len(r.data))
	values := make([][]byte, 0, len(r.data))
	for k, v := range r.data {
		keys = append(keys, k)
		values = append(values, v)
	}
	return &stubIterator{keys: keys, values: values, pos: -1}
}

func (r *RocksDB) NewIteratorWithSnapshot(snapshot *stubSnapshot) *stubIterator {
	return r.NewIterator()
}

func (it *stubIterator) Seek(key []byte) {
	it.pos = 0
	it.valid = it.pos < len(it.keys)
}

func (it *stubIterator) Next() {
	it.pos++
	it.valid = it.pos < len(it.keys)
}

func (it *stubIterator) Valid() bool {
	return it.valid
}

func (it *stubIterator) Key() *stubSlice {
	if !it.valid {
		return &stubSlice{}
	}
	return &stubSlice{data: []byte(it.keys[it.pos])}
}

func (it *stubIterator) Value() *stubSlice {
	if !it.valid {
		return &stubSlice{}
	}
	return &stubSlice{data: it.values[it.pos]}
}

func (it *stubIterator) Err() error {
	return nil
}

func (it *stubIterator) Close() {}

type stubSlice struct {
	data []byte
}

func (s *stubSlice) Data() []byte {
	return s.data
}

func (s *stubSlice) Size() int {
	return len(s.data)
}

func (s *stubSlice) Free() {}

func (s *stubSlice) Exists() bool {
	return len(s.data) > 0
}

func (r *RocksDB) Flush() error {
	return r.save()
}

func (r *RocksDB) CompactRange(start, end []byte) {}

func (r *RocksDB) GetProperty(name string) string {
	return ""
}

func (r *RocksDB) IngestExternalFile(paths []string) error {
	return fmt.Errorf("SST ingest not supported in stub implementation")
}

func (r *RocksDB) Close() {
	_ = r.save()
}

func (r *RocksDB) Stats() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]string{
		"rocksdb.estimate-num-keys":       fmt.Sprintf("%d", len(r.data)),
		"rocksdb.estimate-live-data-size": "0",
		"rocksdb.stats":                   "stub implementation (file-backed)",
	}
}
