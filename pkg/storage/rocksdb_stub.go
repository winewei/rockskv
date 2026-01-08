//go:build !cgo || nocgo
// +build !cgo nocgo

package storage

import (
	"fmt"
)

// RocksDB stub implementation for environments without cgo/RocksDB
// This allows the code to compile and run tests without RocksDB installed

type RocksDB struct {
	config *RocksDBConfig
	data   map[string][]byte
}

func NewRocksDB(config *RocksDBConfig) (*RocksDB, error) {
	if config == nil {
		config = DefaultRocksDBConfig()
	}
	return &RocksDB{
		config: config,
		data:   make(map[string][]byte),
	}, nil
}

func (r *RocksDB) Get(key []byte) ([]byte, bool, error) {
	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result, true, nil
}

func (r *RocksDB) Put(key, value []byte) error {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	r.data[string(key)] = valueCopy
	return nil
}

func (r *RocksDB) Delete(key []byte) error {
	delete(r.data, string(key))
	return nil
}

func (r *RocksDB) BatchPut(items []KeyValueItem) error {
	for _, item := range items {
		if err := r.Put(item.Key, item.Value); err != nil {
			return err
		}
	}
	return nil
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
	return nil
}

func (r *RocksDB) CompactRange(start, end []byte) {}

func (r *RocksDB) GetProperty(name string) string {
	return ""
}

func (r *RocksDB) IngestExternalFile(paths []string) error {
	return fmt.Errorf("SST ingest not supported in stub implementation")
}

func (r *RocksDB) Close() {}

func (r *RocksDB) Stats() map[string]string {
	return map[string]string{
		"rocksdb.estimate-num-keys":       fmt.Sprintf("%d", len(r.data)),
		"rocksdb.estimate-live-data-size": "0",
		"rocksdb.stats":                   "stub implementation",
	}
}
