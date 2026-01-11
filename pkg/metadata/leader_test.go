package metadata

import (
	"testing"
)

func TestLeaderElectionConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  *LeaderElectionConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "empty node ID",
			config: &LeaderElectionConfig{
				NodeID: "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewLeaderElection(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLeaderElection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLeaderElectionWithValidConfig(t *testing.T) {
	// This test will succeed if etcd is running, skip otherwise
	config := &LeaderElectionConfig{
		NodeID:     "test-node",
		EtcdConfig: DefaultEtcdConfig(),
	}

	le, err := NewLeaderElection(config)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
		return
	}
	defer le.client.Close()

	// Verify initial state
	if le.nodeID != "test-node" {
		t.Errorf("nodeID = %v, want test-node", le.nodeID)
	}

	if le.IsLeader() {
		t.Error("should not be leader before starting election")
	}

	if le.GetLeaderID() != "" {
		t.Errorf("leader ID should be empty before election, got %v", le.GetLeaderID())
	}
}

func TestLeaderElectionConstants(t *testing.T) {
	// Test that constants are set correctly
	if leaderElectionPrefix != "/rockskv/leader/" {
		t.Errorf("leaderElectionPrefix = %v, want /rockskv/leader/", leaderElectionPrefix)
	}

	if electionSessionTTL != 10 {
		t.Errorf("electionSessionTTL = %v, want 10", electionSessionTTL)
	}

	if metadataLeaderKey != "/rockskv/leader/metadata" {
		t.Errorf("metadataLeaderKey = %v, want /rockskv/leader/metadata", metadataLeaderKey)
	}
}
