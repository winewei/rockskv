package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "compute.yaml")

	configContent := `
node_id: "test-compute-1"
listen_addr: ":18000"
metadata_addr: "localhost:19000"

pool:
  max_conns_per_host: 20
  idle_timeout: "10m"
  dial_timeout: "10s"
  keep_alive_time: "30s"
  keep_alive_timeout: "10s"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}

	// Verify fields
	if config.NodeID != "test-compute-1" {
		t.Errorf("NodeID: got %q, expected %q", config.NodeID, "test-compute-1")
	}
	if config.ListenAddr != ":18000" {
		t.Errorf("ListenAddr: got %q, expected %q", config.ListenAddr, ":18000")
	}
	if config.MetadataAddr != "localhost:19000" {
		t.Errorf("MetadataAddr: got %q, expected %q", config.MetadataAddr, "localhost:19000")
	}
	if config.Pool.MaxConnsPerHost != 20 {
		t.Errorf("Pool.MaxConnsPerHost: got %d, expected 20", config.Pool.MaxConnsPerHost)
	}
	if config.Pool.IdleTimeout != 10*time.Minute {
		t.Errorf("Pool.IdleTimeout: got %v, expected 10m", config.Pool.IdleTimeout)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "minimal.yaml")

	configContent := `
node_id: "minimal-compute"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}

	// Verify defaults
	if config.ListenAddr == "" {
		t.Error("ListenAddr should have default value")
	}
	if config.MetadataAddr == "" {
		t.Error("MetadataAddr should have default value")
	}
	if config.Pool == nil {
		t.Fatal("Pool config should not be nil")
	}
	if config.Pool.MaxConnsPerHost == 0 {
		t.Error("Pool.MaxConnsPerHost should have default value")
	}
	if config.Pool.DialTimeout == 0 {
		t.Error("Pool.DialTimeout should have default value")
	}
}

func TestLoadConfigWithLocalFile(t *testing.T) {
	configPath := "../../config/local/compute.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Local config file not found, skipping")
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig with local file failed: %v", err)
	}

	// Critical fields must not be empty
	if config.NodeID == "" {
		t.Error("NodeID is empty")
	}
	if config.ListenAddr == "" {
		t.Error("ListenAddr is empty")
	}
	if config.MetadataAddr == "" {
		t.Error("MetadataAddr is empty")
	}
	if config.Pool == nil {
		t.Error("Pool config is nil")
	}
}
