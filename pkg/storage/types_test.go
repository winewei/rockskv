package storage

import (
	"runtime"
	"testing"
)

func TestDefaultRocksDBConfig(t *testing.T) {
	config := DefaultRocksDBConfig()

	if config == nil {
		t.Fatal("DefaultRocksDBConfig returned nil")
	}

	// Verify default values
	if config.DataDir != "/data/rockskv" {
		t.Errorf("DataDir = %s, want /data/rockskv", config.DataDir)
	}

	if config.WALDir != "/data/rockskv/wal" {
		t.Errorf("WALDir = %s, want /data/rockskv/wal", config.WALDir)
	}

	if config.BlockCacheSize != 512*1024*1024 {
		t.Errorf("BlockCacheSize = %d, want %d", config.BlockCacheSize, 512*1024*1024)
	}

	if config.WriteBufferSize != 64*1024*1024 {
		t.Errorf("WriteBufferSize = %d, want %d", config.WriteBufferSize, 64*1024*1024)
	}

	if config.MaxWriteBufferNumber != 4 {
		t.Errorf("MaxWriteBufferNumber = %d, want 4", config.MaxWriteBufferNumber)
	}

	expectedCompactions := runtime.NumCPU() / 2
	if config.MaxBackgroundCompactions != expectedCompactions {
		t.Errorf("MaxBackgroundCompactions = %d, want %d", config.MaxBackgroundCompactions, expectedCompactions)
	}

	if config.MaxBackgroundFlushes != 2 {
		t.Errorf("MaxBackgroundFlushes = %d, want 2", config.MaxBackgroundFlushes)
	}

	if config.TargetFileSizeBase != 64*1024*1024 {
		t.Errorf("TargetFileSizeBase = %d, want %d", config.TargetFileSizeBase, 64*1024*1024)
	}

	if config.MaxBytesForLevelBase != 256*1024*1024 {
		t.Errorf("MaxBytesForLevelBase = %d, want %d", config.MaxBytesForLevelBase, 256*1024*1024)
	}

	if config.CompressionType != "lz4" {
		t.Errorf("CompressionType = %s, want lz4", config.CompressionType)
	}
}

func TestRocksDBConfigCustomValues(t *testing.T) {
	config := &RocksDBConfig{
		DataDir:                  "/custom/data",
		WALDir:                   "/custom/wal",
		BlockCacheSize:           1024 * 1024 * 1024,
		WriteBufferSize:          128 * 1024 * 1024,
		MaxWriteBufferNumber:     8,
		MaxBackgroundCompactions: 4,
		MaxBackgroundFlushes:     4,
		TargetFileSizeBase:       128 * 1024 * 1024,
		MaxBytesForLevelBase:     512 * 1024 * 1024,
		CompressionType:          "zstd",
	}

	if config.DataDir != "/custom/data" {
		t.Error("Custom DataDir not set correctly")
	}

	if config.CompressionType != "zstd" {
		t.Error("Custom CompressionType not set correctly")
	}
}

func TestKeyValueItem(t *testing.T) {
	item := KeyValueItem{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	if string(item.Key) != "test-key" {
		t.Errorf("Key = %s, want test-key", item.Key)
	}

	if string(item.Value) != "test-value" {
		t.Errorf("Value = %s, want test-value", item.Value)
	}
}

func TestKeyValueItemEmpty(t *testing.T) {
	item := KeyValueItem{
		Key:   []byte{},
		Value: []byte{},
	}

	if len(item.Key) != 0 {
		t.Error("Empty key should have length 0")
	}

	if len(item.Value) != 0 {
		t.Error("Empty value should have length 0")
	}
}

func TestKeyValueItemBinary(t *testing.T) {
	item := KeyValueItem{
		Key:   []byte{0x00, 0x01, 0x02, 0xff},
		Value: []byte{0xde, 0xad, 0xbe, 0xef},
	}

	if len(item.Key) != 4 {
		t.Errorf("Binary key length = %d, want 4", len(item.Key))
	}

	if item.Value[0] != 0xde {
		t.Errorf("Binary value[0] = %x, want 0xde", item.Value[0])
	}
}
