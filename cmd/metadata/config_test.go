package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "metadata.yaml")

	configContent := `
node_id: "test-metadata-1"
listen_addr: ":19000"
ha_enabled: true

etcd:
  endpoints:
    - "localhost:12379"
    - "localhost:22379"
  dial_timeout: "10s"
  username: "testuser"
  password: "testpass"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}

	// Verify fields
	if config.ServerConfig.NodeID != "test-metadata-1" {
		t.Errorf("NodeID: got %q, expected %q", config.ServerConfig.NodeID, "test-metadata-1")
	}
	if config.ServerConfig.ListenAddr != ":19000" {
		t.Errorf("ListenAddr: got %q, expected %q", config.ServerConfig.ListenAddr, ":19000")
	}
	if !config.HAEnabled {
		t.Error("HAEnabled should be true")
	}
	if len(config.ServerConfig.Etcd.Endpoints) != 2 {
		t.Errorf("Etcd.Endpoints: got %d, expected 2", len(config.ServerConfig.Etcd.Endpoints))
	}
	if config.ServerConfig.Etcd.DialTimeout != 10*time.Second {
		t.Errorf("Etcd.DialTimeout: got %v, expected 10s", config.ServerConfig.Etcd.DialTimeout)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "minimal.yaml")

	configContent := `
node_id: "minimal-metadata"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig failed: %v", err)
	}

	// Verify defaults
	if config.ServerConfig.ListenAddr == "" {
		t.Error("ListenAddr should have default value")
	}
	if len(config.ServerConfig.Etcd.Endpoints) == 0 {
		t.Error("Etcd.Endpoints should have default value")
	}
	if config.HAEnabled {
		t.Error("HAEnabled should default to false")
	}
}

func TestLoadConfigWithLocalFile(t *testing.T) {
	configPath := "../../config/local/metadata.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Local config file not found, skipping")
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig with local file failed: %v", err)
	}

	// Critical fields must not be empty
	if config.ServerConfig.NodeID == "" {
		t.Error("NodeID is empty")
	}
	if config.ServerConfig.ListenAddr == "" {
		t.Error("ListenAddr is empty")
	}
	if config.ServerConfig.Etcd == nil || len(config.ServerConfig.Etcd.Endpoints) == 0 {
		t.Error("Etcd.Endpoints is empty")
	}
}
