package metadata

import (
	"strings"
	"testing"
	"time"
)

// TestEtcdConnectionFailFast verifies that NewEtcdStore fails quickly when etcd is unavailable
// instead of hanging until timeout. This is a critical property for early error detection.
func TestEtcdConnectionFailFast(t *testing.T) {
	// Use an unreachable endpoint (assuming nothing is running on port 12345)
	config := &EtcdConfig{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 5 * time.Second,
	}

	start := time.Now()
	_, err := NewEtcdStore(config)
	elapsed := time.Since(start)

	// Should fail
	if err == nil {
		t.Fatal("Expected error when connecting to unavailable etcd, got nil")
	}

	// Should fail within reasonable time (we use 3s timeout + some overhead)
	// If it takes close to DialTimeout (5s), the fail-fast mechanism isn't working
	maxExpectedDuration := 4 * time.Second // 3s status check + 1s overhead
	if elapsed > maxExpectedDuration {
		t.Errorf("Connection check took too long: %v (expected < %v). Fail-fast may not be working.",
			elapsed, maxExpectedDuration)
	}

	// Error message should mention the verification failure
	if !strings.Contains(err.Error(), "failed to verify etcd connection") &&
		!strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("Expected error message about verification failure, got: %v", err)
	}

	t.Logf("✅ Fail-fast working correctly: failed in %v with error: %v", elapsed, err)
}

// TestEtcdConnectionSuccess verifies that NewEtcdStore succeeds when etcd is available
// This test requires a running etcd instance
func TestEtcdConnectionSuccess(t *testing.T) {
	t.Skip("Requires running etcd - run with integration tests")

	config := &EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	start := time.Now()
	store, err := NewEtcdStore(config)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Expected successful connection to etcd, got error: %v", err)
	}
	defer store.Close()

	// Should connect quickly when etcd is available
	maxExpectedDuration := 2 * time.Second
	if elapsed > maxExpectedDuration {
		t.Errorf("Connection took longer than expected: %v (expected < %v)",
			elapsed, maxExpectedDuration)
	}

	t.Logf("✅ Connected to etcd successfully in %v", elapsed)
}
