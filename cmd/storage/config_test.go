package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "storage.yaml")

	configContent := `
node_id: "test-storage-1"
listen_addr: ":19001"
metadata_addr: "localhost:19000"
etcd_endpoints:
  - "localhost:12379"
  - "localhost:22379"
sst_dir: "./test-sst"
command_log_dir: "./test-cmdlog"

rocksdb:
  data_dir: "./test-data"
  wal_dir: "./test-data/wal"
  block_cache_size: 134217728
  write_buffer_size: 16777216
  max_write_buffer_number: 2
  max_background_compactions: 2
  max_background_flushes: 1
  compression_type: "snappy"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}

	// Verify EtcdEndpoints separately (slice comparison)
	if len(config.EtcdEndpoints) != 2 {
		t.Errorf("EtcdEndpoints: got %d endpoints, expected 2", len(config.EtcdEndpoints))
	}

	// Verify all fields are loaded correctly
	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"NodeID", config.NodeID, "test-storage-1"},
		{"ListenAddr", config.ListenAddr, ":19001"},
		{"MetadataAddr", config.MetadataAddr, "localhost:19000"},
		{"SSTDir", config.SSTDir, "./test-sst"},
		{"CommandLogDir", config.CommandLogDir, "./test-cmdlog"},
		{"RocksDB.DataDir", config.RocksDB.DataDir, "./test-data"},
		{"RocksDB.WALDir", config.RocksDB.WALDir, "./test-data/wal"},
		{"RocksDB.BlockCacheSize", config.RocksDB.BlockCacheSize, int64(134217728)},
		{"RocksDB.WriteBufferSize", config.RocksDB.WriteBufferSize, uint64(16777216)},
		{"RocksDB.MaxWriteBufferNumber", config.RocksDB.MaxWriteBufferNumber, 2},
		{"RocksDB.MaxBackgroundCompactions", config.RocksDB.MaxBackgroundCompactions, 2},
		{"RocksDB.MaxBackgroundFlushes", config.RocksDB.MaxBackgroundFlushes, 1},
		{"RocksDB.CompressionType", config.RocksDB.CompressionType, "snappy"},
	}

	for _, tt := range tests {
		if tt.got != tt.expected {
			t.Errorf("%s: got %v, expected %v", tt.name, tt.got, tt.expected)
		}
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	// Create a minimal config file to test defaults
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "minimal.yaml")

	// Empty config - should use all defaults
	configContent := `
node_id: "minimal-node"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}

	// Verify critical fields have defaults (not empty)
	// Verify EtcdEndpoints has default
	if len(config.EtcdEndpoints) == 0 {
		t.Error("EtcdEndpoints should have default value")
	}

	criticalFields := []struct {
		name  string
		value string
	}{
		{"ListenAddr", config.ListenAddr},
		{"MetadataAddr", config.MetadataAddr},
		{"SSTDir", config.SSTDir},
		{"CommandLogDir", config.CommandLogDir},
		{"RocksDB.DataDir", config.RocksDB.DataDir},
		{"RocksDB.WALDir", config.RocksDB.WALDir},
	}

	for _, f := range criticalFields {
		if f.value == "" {
			t.Errorf("%s should have a default value, got empty string", f.name)
		}
	}

	// Verify specific defaults
	if config.CommandLogDir != "/tmp/rockskv/cmdlog" {
		t.Errorf("CommandLogDir default: got %q, expected %q", config.CommandLogDir, "/tmp/rockskv/cmdlog")
	}
	if config.SSTDir != "/tmp/rockskv/sst" {
		t.Errorf("SSTDir default: got %q, expected %q", config.SSTDir, "/tmp/rockskv/sst")
	}
}

func TestLoadConfigWithLocalFile(t *testing.T) {
	// Test with actual local config file if it exists
	configPath := "../../config/local/storage.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Local config file not found, skipping")
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig with local file failed: %v", err)
	}

	// Critical fields must not be empty after loading real config
	if config.CommandLogDir == "" {
		t.Error("CommandLogDir is empty after loading local config - this will cause service startup failure")
	}
	if config.SSTDir == "" {
		t.Error("SSTDir is empty after loading local config")
	}
	if len(config.EtcdEndpoints) == 0 {
		t.Error("EtcdEndpoints is empty after loading local config - this will cause service startup failure")
	}
	if config.RocksDB == nil {
		t.Fatal("RocksDB config is nil")
	}
	if config.RocksDB.DataDir == "" {
		t.Error("RocksDB.DataDir is empty")
	}
}
