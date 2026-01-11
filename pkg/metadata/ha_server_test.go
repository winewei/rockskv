package metadata

import (
	"testing"
)

func TestDefaultHAServerConfig(t *testing.T) {
	config := DefaultHAServerConfig()

	if config == nil {
		t.Fatal("DefaultHAServerConfig() returned nil")
	}

	if config.ServerConfig == nil {
		t.Error("ServerConfig is nil")
	}

	if config.HAEnabled != false {
		t.Errorf("HAEnabled = %v, want false", config.HAEnabled)
	}
}

func TestHAServerConfigFields(t *testing.T) {
	config := &HAServerConfig{
		ServerConfig: &ServerConfig{
			NodeID:     "test-node",
			ListenAddr: ":9000",
		},
		HAEnabled: true,
	}

	if config.NodeID != "test-node" {
		t.Errorf("NodeID = %v, want test-node", config.NodeID)
	}

	if config.ListenAddr != ":9000" {
		t.Errorf("ListenAddr = %v, want :9000", config.ListenAddr)
	}

	if !config.HAEnabled {
		t.Error("HAEnabled should be true")
	}
}

func TestHAServerConfigWithHADisabled(t *testing.T) {
	config := &HAServerConfig{
		ServerConfig: &ServerConfig{
			NodeID:     "single-node",
			ListenAddr: ":9000",
		},
		HAEnabled: false,
	}

	if config.HAEnabled {
		t.Error("HAEnabled should be false for single-node mode")
	}
}

func TestHAServerConfigMultipleNodes(t *testing.T) {
	// Test configuration for multiple HA nodes
	configs := []*HAServerConfig{
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-1",
				ListenAddr: ":9000",
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-2",
				ListenAddr: ":9010",
			},
			HAEnabled: true,
		},
		{
			ServerConfig: &ServerConfig{
				NodeID:     "metadata-3",
				ListenAddr: ":9020",
			},
			HAEnabled: true,
		},
	}

	// Verify each node has unique ID and address
	nodeIDs := make(map[string]bool)
	addrs := make(map[string]bool)

	for _, cfg := range configs {
		if nodeIDs[cfg.NodeID] {
			t.Errorf("Duplicate node ID: %s", cfg.NodeID)
		}
		nodeIDs[cfg.NodeID] = true

		if addrs[cfg.ListenAddr] {
			t.Errorf("Duplicate listen address: %s", cfg.ListenAddr)
		}
		addrs[cfg.ListenAddr] = true

		if !cfg.HAEnabled {
			t.Errorf("Node %s should have HA enabled", cfg.NodeID)
		}
	}

	if len(nodeIDs) != 3 {
		t.Errorf("Expected 3 unique node IDs, got %d", len(nodeIDs))
	}
}
