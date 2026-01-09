package storage

import (
	"runtime"
)

// RocksDBConfig holds RocksDB configuration
type RocksDBConfig struct {
	DataDir                  string `mapstructure:"data_dir"`
	WALDir                   string `mapstructure:"wal_dir"`
	BlockCacheSize           int64  `mapstructure:"block_cache_size"`
	WriteBufferSize          uint64 `mapstructure:"write_buffer_size"`
	MaxWriteBufferNumber     int    `mapstructure:"max_write_buffer_number"`
	MaxBackgroundCompactions int    `mapstructure:"max_background_compactions"`
	MaxBackgroundFlushes     int    `mapstructure:"max_background_flushes"`
	TargetFileSizeBase       uint64 `mapstructure:"target_file_size_base"`
	MaxBytesForLevelBase     uint64 `mapstructure:"max_bytes_for_level_base"`
	CompressionType          string `mapstructure:"compression_type"`
}

// DefaultRocksDBConfig returns a default configuration
func DefaultRocksDBConfig() *RocksDBConfig {
	numCPU := runtime.NumCPU()
	return &RocksDBConfig{
		DataDir:                  "/data/rockskv",
		WALDir:                   "/data/rockskv/wal",
		BlockCacheSize:           512 * 1024 * 1024, // 512MB
		WriteBufferSize:          64 * 1024 * 1024,  // 64MB
		MaxWriteBufferNumber:     4,
		MaxBackgroundCompactions: numCPU / 2,
		MaxBackgroundFlushes:     2,
		TargetFileSizeBase:       64 * 1024 * 1024,  // 64MB
		MaxBytesForLevelBase:     256 * 1024 * 1024, // 256MB
		CompressionType:          "lz4",
	}
}

// KeyValueItem represents a key-value pair
type KeyValueItem struct {
	Key   []byte
	Value []byte
}
