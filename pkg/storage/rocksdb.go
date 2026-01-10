//go:build cgo && !nocgo
// +build cgo,!nocgo

package storage

import (
	"fmt"

	"github.com/linxGnu/grocksdb"
	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
)

// RocksDB wraps grocksdb.DB with convenience methods
type RocksDB struct {
	db         *grocksdb.DB
	readOpts   *grocksdb.ReadOptions
	writeOpts  *grocksdb.WriteOptions
	flushOpts  *grocksdb.FlushOptions
	blockCache *grocksdb.Cache
	logger     *zap.Logger
	config     *RocksDBConfig
}

// NewRocksDB creates a new RocksDB instance
func NewRocksDB(config *RocksDBConfig) (*RocksDB, error) {
	logger := common.NewLogger("rocksdb")

	if config == nil {
		config = DefaultRocksDBConfig()
	}

	// Block cache for data blocks
	blockCache := grocksdb.NewLRUCache(uint64(config.BlockCacheSize))

	// Block-based table options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(blockCache)
	bbto.SetBlockSize(16 * 1024) // 16KB blocks
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	bbto.SetCacheIndexAndFilterBlocks(true)
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)

	// Main options
	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	// WAL directory separation (WAL on EBS, data on NVMe)
	if config.WALDir != "" {
		opts.SetWalDir(config.WALDir)
	}

	// Write buffer settings
	opts.SetWriteBufferSize(config.WriteBufferSize)
	opts.SetMaxWriteBufferNumber(config.MaxWriteBufferNumber)

	// Background threads
	opts.SetMaxBackgroundCompactions(config.MaxBackgroundCompactions)
	opts.SetMaxBackgroundFlushes(config.MaxBackgroundFlushes)

	// Level settings
	opts.SetTargetFileSizeBase(config.TargetFileSizeBase)
	opts.SetMaxBytesForLevelBase(config.MaxBytesForLevelBase)

	// Compression
	switch config.CompressionType {
	case "lz4":
		opts.SetCompression(grocksdb.LZ4Compression)
	case "zstd":
		opts.SetCompression(grocksdb.ZSTDCompression)
	case "snappy":
		opts.SetCompression(grocksdb.SnappyCompression)
	default:
		opts.SetCompression(grocksdb.LZ4Compression)
	}

	// Open database
	db, err := grocksdb.OpenDb(opts, config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open rocksdb: %w", err)
	}

	// Read options
	readOpts := grocksdb.NewDefaultReadOptions()
	readOpts.SetVerifyChecksums(true)

	// Write options
	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false) // WAL provides durability

	// Flush options
	flushOpts := grocksdb.NewDefaultFlushOptions()

	logger.Info("RocksDB opened",
		zap.String("data_dir", config.DataDir),
		zap.String("wal_dir", config.WALDir),
		zap.Int64("block_cache_size", config.BlockCacheSize),
	)

	return &RocksDB{
		db:         db,
		readOpts:   readOpts,
		writeOpts:  writeOpts,
		flushOpts:  flushOpts,
		blockCache: blockCache,
		logger:     logger,
		config:     config,
	}, nil
}

// Get retrieves a value by key
func (r *RocksDB) Get(key []byte) ([]byte, bool, error) {
	slice, err := r.db.Get(r.readOpts, key)
	if err != nil {
		return nil, false, fmt.Errorf("get failed: %w", err)
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	// Copy data since slice will be freed
	data := make([]byte, slice.Size())
	copy(data, slice.Data())

	return data, true, nil
}

// Put stores a key-value pair
func (r *RocksDB) Put(key, value []byte) error {
	if err := r.db.Put(r.writeOpts, key, value); err != nil {
		return fmt.Errorf("put failed: %w", err)
	}
	return nil
}

// Delete removes a key
func (r *RocksDB) Delete(key []byte) error {
	if err := r.db.Delete(r.writeOpts, key); err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

// BatchPut stores multiple key-value pairs atomically
func (r *RocksDB) BatchPut(items []KeyValueItem) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, item := range items {
		batch.Put(item.Key, item.Value)
	}

	if err := r.db.Write(r.writeOpts, batch); err != nil {
		return fmt.Errorf("batch put failed: %w", err)
	}
	return nil
}

// CreateSnapshot creates a read-only snapshot
func (r *RocksDB) CreateSnapshot() *grocksdb.Snapshot {
	return r.db.NewSnapshot()
}

// ReleaseSnapshot releases a snapshot
func (r *RocksDB) ReleaseSnapshot(snapshot *grocksdb.Snapshot) {
	r.db.ReleaseSnapshot(snapshot)
}

// NewIterator creates a new iterator
func (r *RocksDB) NewIterator() *grocksdb.Iterator {
	return r.db.NewIterator(r.readOpts)
}

// NewIteratorWithSnapshot creates a new iterator with snapshot
func (r *RocksDB) NewIteratorWithSnapshot(snapshot *grocksdb.Snapshot) *grocksdb.Iterator {
	readOpts := grocksdb.NewDefaultReadOptions()
	readOpts.SetSnapshot(snapshot)
	return r.db.NewIterator(readOpts)
}

// Flush forces a memtable flush
func (r *RocksDB) Flush() error {
	if err := r.db.Flush(r.flushOpts); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}
	return nil
}

// CompactRange compacts a key range
func (r *RocksDB) CompactRange(start, end []byte) {
	r.db.CompactRange(grocksdb.Range{Start: start, Limit: end})
}

// GetProperty returns a RocksDB property
func (r *RocksDB) GetProperty(name string) string {
	return r.db.GetProperty(name)
}

// IngestExternalFile ingests SST files
func (r *RocksDB) IngestExternalFile(paths []string) error {
	ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
	defer ingestOpts.Destroy()

	if err := r.db.IngestExternalFile(paths, ingestOpts); err != nil {
		return fmt.Errorf("ingest external file failed: %w", err)
	}

	r.logger.Info("Ingested SST files",
		zap.Strings("paths", paths),
	)

	return nil
}

// Close closes the database
func (r *RocksDB) Close() {
	if r.readOpts != nil {
		r.readOpts.Destroy()
	}
	if r.writeOpts != nil {
		r.writeOpts.Destroy()
	}
	if r.flushOpts != nil {
		r.flushOpts.Destroy()
	}
	if r.db != nil {
		r.db.Close()
	}
	if r.blockCache != nil {
		r.blockCache.Destroy()
	}
	r.logger.Info("RocksDB closed")
}

// Stats returns database statistics
func (r *RocksDB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["rocksdb.estimate-num-keys"] = r.db.GetProperty("rocksdb.estimate-num-keys")
	stats["rocksdb.estimate-live-data-size"] = r.db.GetProperty("rocksdb.estimate-live-data-size")
	stats["rocksdb.stats"] = r.db.GetProperty("rocksdb.stats")
	return stats
}
