package client

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	if len(config.ComputeAddrs) != 1 {
		t.Errorf("ComputeAddrs length = %d, want 1", len(config.ComputeAddrs))
	}

	if config.ComputeAddrs[0] != "localhost:8000" {
		t.Errorf("ComputeAddrs[0] = %s, want localhost:8000", config.ComputeAddrs[0])
	}

	if config.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %v, want 5s", config.DialTimeout)
	}

	if config.RequestTimeout != 5*time.Second {
		t.Errorf("RequestTimeout = %v, want 5s", config.RequestTimeout)
	}

	if config.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", config.MaxRetries)
	}

	if config.RetryInterval != 100*time.Millisecond {
		t.Errorf("RetryInterval = %v, want 100ms", config.RetryInterval)
	}
}

func TestConfigCustomValues(t *testing.T) {
	config := &Config{
		ComputeAddrs:   []string{"node1:8000", "node2:8000"},
		DialTimeout:    10 * time.Second,
		RequestTimeout: 15 * time.Second,
		MaxRetries:     5,
		RetryInterval:  200 * time.Millisecond,
	}

	if len(config.ComputeAddrs) != 2 {
		t.Errorf("ComputeAddrs length = %d, want 2", len(config.ComputeAddrs))
	}

	if config.DialTimeout != 10*time.Second {
		t.Errorf("DialTimeout = %v, want 10s", config.DialTimeout)
	}

	if config.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", config.MaxRetries)
	}
}

func TestNewClientEmptyAddrs(t *testing.T) {
	config := &Config{
		ComputeAddrs:   []string{},
		DialTimeout:    5 * time.Second,
		RequestTimeout: 5 * time.Second,
	}

	_, err := New(config)
	if err == nil {
		t.Error("expected error with empty compute addresses")
	}
}

func TestNewClientNilConfig(t *testing.T) {
	// This will try to connect to localhost:8000 which should fail
	// but it should not panic due to nil config
	_, err := New(nil)
	// We expect an error because we can't connect, but not a nil pointer panic
	if err == nil {
		// If no error, that's also fine - it means connection succeeded
		t.Log("Successfully connected with nil config (using defaults)")
	}
}

func TestClientClose(t *testing.T) {
	// Test that Close doesn't panic on a client with nil conn
	c := &Client{
		conn: nil,
	}

	err := c.Close()
	if err != nil {
		t.Errorf("Close with nil conn returned error: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		shouldError bool
	}{
		{
			name: "valid config",
			config: &Config{
				ComputeAddrs:   []string{"localhost:8000"},
				DialTimeout:    5 * time.Second,
				RequestTimeout: 5 * time.Second,
				MaxRetries:     3,
				RetryInterval:  100 * time.Millisecond,
			},
			shouldError: false,
		},
		{
			name: "empty addrs",
			config: &Config{
				ComputeAddrs:   []string{},
				DialTimeout:    5 * time.Second,
				RequestTimeout: 5 * time.Second,
			},
			shouldError: true,
		},
		{
			name:        "nil config uses defaults",
			config:      nil,
			shouldError: false, // Should use defaults and try to connect
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.config)
			if tt.shouldError && err == nil {
				t.Error("expected error but got none")
			}
			// Note: we can't fully test connection success without a running server
		})
	}
}

func TestConfigMultipleAddrs(t *testing.T) {
	config := &Config{
		ComputeAddrs: []string{
			"node1:8000",
			"node2:8000",
			"node3:8000",
		},
		DialTimeout:    100 * time.Millisecond, // Short timeout for testing
		RequestTimeout: 5 * time.Second,
		MaxRetries:     3,
		RetryInterval:  100 * time.Millisecond,
	}

	if len(config.ComputeAddrs) != 3 {
		t.Errorf("Expected 3 addresses, got %d", len(config.ComputeAddrs))
	}
}

func BenchmarkDefaultConfig(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = DefaultConfig()
	}
}
