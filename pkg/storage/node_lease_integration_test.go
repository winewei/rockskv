//go:build integration
// +build integration

package storage

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	testEtcdEndpoint = "localhost:2379"
)

// TestNodeLeaseAcquisition verifies that the server successfully acquires a node-level lease from etcd
func TestNodeLeaseAcquisition(t *testing.T) {
	// Create temporary directory for test data
	dataDir := t.TempDir()

	config := &ServerConfig{
		NodeID:         "test-node-lease-1",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir + "/sst",
		CommandLogDir:  dataDir + "/cmdlog",
		EtcdEndpoints:  []string{testEtcdEndpoint},
	}
	config.RocksDB.DataDir = dataDir
	config.RocksDB.WALDir = dataDir + "/wal"

	// Create server - should acquire node lease
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Verify node lease was acquired
	if server.nodeLease == 0 {
		t.Fatalf("Node lease was not acquired (nodeLease=0)")
	}

	// Verify lease is active
	if server.nodeLeaseLost {
		t.Fatalf("Node lease is marked as lost immediately after acquisition")
	}

	// Verify canWrite() returns true with valid lease
	if !server.canWrite() {
		t.Fatalf("canWrite() returned false with valid node lease")
	}

	// Verify lease exists in etcd
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ttlResp, err := server.etcdClient.TimeToLive(ctx, server.nodeLease)
	if err != nil {
		t.Fatalf("Failed to check lease TTL: %v", err)
	}
	if ttlResp.TTL <= 0 {
		t.Fatalf("Lease TTL is invalid: %d", ttlResp.TTL)
	}

	t.Logf("✅ Node lease acquired successfully: ID=%d, TTL=%ds", server.nodeLease, ttlResp.TTL)
}

// TestNodeLeaseRenewal verifies that the KeepAlive stream continuously renews the lease
func TestNodeLeaseRenewal(t *testing.T) {
	dataDir := t.TempDir()

	config := &ServerConfig{
		NodeID:         "test-node-lease-2",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir + "/sst",
		CommandLogDir:  dataDir + "/cmdlog",
		EtcdEndpoints:  []string{testEtcdEndpoint},
	}
	config.RocksDB.DataDir = dataDir
	config.RocksDB.WALDir = dataDir + "/wal"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	initialLeaseID := server.nodeLease
	if initialLeaseID == 0 {
		t.Fatalf("Node lease was not acquired")
	}

	// Wait for multiple lease renewal cycles (TTL is 3 seconds)
	// The KeepAlive stream should continuously renew the lease
	time.Sleep(5 * time.Second)

	// Verify lease is still active
	if server.nodeLeaseLost {
		t.Fatalf("Node lease was lost during renewal period")
	}

	// Verify lease ID is unchanged (same lease, continuously renewed)
	if server.nodeLease != initialLeaseID {
		t.Fatalf("Lease ID changed from %d to %d", initialLeaseID, server.nodeLease)
	}

	// Verify lease still exists in etcd with valid TTL
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ttlResp, err := server.etcdClient.TimeToLive(ctx, server.nodeLease)
	if err != nil {
		t.Fatalf("Failed to check lease TTL: %v", err)
	}
	if ttlResp.TTL <= 0 {
		t.Fatalf("Lease expired during renewal period (TTL=%d)", ttlResp.TTL)
	}

	t.Logf("✅ Lease renewed successfully via KeepAlive stream: ID=%d, TTL=%ds", server.nodeLease, ttlResp.TTL)
}

// TestNodeLeaseLossEntersReadOnly verifies that the server enters read-only mode when lease is lost
func TestNodeLeaseLossEntersReadOnly(t *testing.T) {
	dataDir := t.TempDir()

	config := &ServerConfig{
		NodeID:         "test-node-lease-3",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir + "/sst",
		CommandLogDir:  dataDir + "/cmdlog",
		EtcdEndpoints:  []string{testEtcdEndpoint},
	}
	config.RocksDB.DataDir = dataDir
	config.RocksDB.WALDir = dataDir + "/wal"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	leaseID := server.nodeLease
	if leaseID == 0 {
		t.Fatalf("Node lease was not acquired")
	}

	// Verify initial state: lease is active, writes are allowed
	if server.nodeLeaseLost {
		t.Fatalf("Lease marked as lost initially")
	}
	if !server.canWrite() {
		t.Fatalf("canWrite() returned false with valid lease")
	}

	// Revoke the lease to simulate network partition or etcd failure
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.etcdClient.Revoke(ctx, leaseID)
	if err != nil {
		t.Fatalf("Failed to revoke lease: %v", err)
	}

	// Wait for monitorNodeLease() to detect the lease loss
	// The KeepAlive channel should close when lease is revoked
	time.Sleep(2 * time.Second)

	// Verify server detected lease loss
	if !server.nodeLeaseLost {
		t.Fatalf("Server did not detect lease loss (nodeLeaseLost=false)")
	}

	// Verify canWrite() returns false after lease loss
	if server.canWrite() {
		t.Fatalf("canWrite() returned true after lease was lost")
	}

	t.Logf("✅ Server correctly entered read-only mode after lease loss")
}

// TestProductionModeRequiresEtcd verifies that production mode fails without etcd
func TestProductionModeRequiresEtcd(t *testing.T) {
	dataDir := t.TempDir()

	config := &ServerConfig{
		NodeID:         "test-node-no-etcd",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir + "/sst",
		CommandLogDir:  dataDir + "/cmdlog",
		EtcdEndpoints:  []string{}, // No etcd endpoints
	}
	config.RocksDB.DataDir = dataDir
	config.RocksDB.WALDir = dataDir + "/wal"

	// Should fail to create server without etcd
	_, err := NewServer(config)
	if err == nil {
		t.Fatalf("Server creation should have failed without etcd")
	}

	expectedMsg := "etcd endpoints required for node-level lease management"
	if err.Error() != expectedMsg {
		t.Fatalf("Unexpected error message.\nExpected: %s\nGot: %s", expectedMsg, err.Error())
	}

	t.Logf("✅ Server correctly requires etcd endpoints")
}

// TestKeepAliveVsKeepAliveOnce verifies that we're using KeepAlive (stream reuse) not KeepAliveOnce
func TestKeepAliveVsKeepAliveOnce(t *testing.T) {
	dataDir := t.TempDir()

	config := &ServerConfig{
		NodeID:         "test-node-keepalive",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir + "/sst",
		CommandLogDir:  dataDir + "/cmdlog",
		EtcdEndpoints:  []string{testEtcdEndpoint},
	}
	config.RocksDB.DataDir = dataDir
	config.RocksDB.WALDir = dataDir + "/wal"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Verify leaseKeepalive channel exists (KeepAlive stream)
	if server.leaseKeepalive == nil {
		t.Fatalf("leaseKeepalive channel is nil - not using KeepAlive stream")
	}

	// Wait and verify the channel is still receiving keepalive responses
	time.Sleep(4 * time.Second)

	// Lease should still be alive after multiple TTL periods
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ttlResp, err := server.etcdClient.TimeToLive(ctx, server.nodeLease)
	if err != nil {
		t.Fatalf("Failed to check lease TTL: %v", err)
	}
	if ttlResp.TTL <= 0 {
		t.Fatalf("Lease expired - KeepAlive stream may not be working")
	}

	t.Logf("✅ KeepAlive stream is working correctly (lease ID=%d, TTL=%ds)", server.nodeLease, ttlResp.TTL)
}

// TestEtcdConnectionFailure verifies behavior when etcd is unreachable
func TestEtcdConnectionFailure(t *testing.T) {
	dataDir := t.TempDir()

	config := &ServerConfig{
		NodeID:         "test-node-bad-etcd",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir + "/sst",
		CommandLogDir:  dataDir + "/cmdlog",
		EtcdEndpoints:  []string{"localhost:19999"}, // Invalid endpoint
	}
	config.RocksDB.DataDir = dataDir
	config.RocksDB.WALDir = dataDir + "/wal"

	// Should fail to create server when etcd is unreachable
	server, err := NewServer(config)
	if err == nil {
		if server != nil {
			server.Stop()
		}
		t.Fatalf("Server creation should have failed with unreachable etcd")
	}

	t.Logf("✅ Server correctly failed to start with unreachable etcd: %v", err)
}

// TestMultipleServersIndependentLeases verifies that multiple storage nodes each get their own lease
func TestMultipleServersIndependentLeases(t *testing.T) {
	dataDir1 := t.TempDir()
	dataDir2 := t.TempDir()

	config1 := &ServerConfig{
		NodeID:         "test-node-multi-1",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir1 + "/sst",
		CommandLogDir:  dataDir1 + "/cmdlog",
		EtcdEndpoints:  []string{testEtcdEndpoint},
	}
	config1.RocksDB.DataDir = dataDir1
	config1.RocksDB.WALDir = dataDir1 + "/wal"

	config2 := &ServerConfig{
		NodeID:         "test-node-multi-2",
		ListenAddr:     ":0",
		MetadataAddr:   "localhost:9000",
		RocksDB:        DefaultRocksDBConfig(),
		SSTDir:         dataDir2 + "/sst",
		CommandLogDir:  dataDir2 + "/cmdlog",
		EtcdEndpoints:  []string{testEtcdEndpoint},
	}
	config2.RocksDB.DataDir = dataDir2
	config2.RocksDB.WALDir = dataDir2 + "/wal"

	// Create two servers
	server1, err := NewServer(config1)
	if err != nil {
		t.Fatalf("Failed to create server1: %v", err)
	}
	defer server1.Stop()

	server2, err := NewServer(config2)
	if err != nil {
		t.Fatalf("Failed to create server2: %v", err)
	}
	defer server2.Stop()

	// Verify both servers acquired different leases
	if server1.nodeLease == 0 {
		t.Fatalf("Server1 did not acquire lease")
	}
	if server2.nodeLease == 0 {
		t.Fatalf("Server2 did not acquire lease")
	}

	if server1.nodeLease == server2.nodeLease {
		t.Fatalf("Both servers acquired the same lease ID: %d", server1.nodeLease)
	}

	// Verify both can write
	if !server1.canWrite() {
		t.Fatalf("Server1 cannot write")
	}
	if !server2.canWrite() {
		t.Fatalf("Server2 cannot write")
	}

	// Verify both leases exist in etcd
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	ttl1, err := etcdClient.TimeToLive(ctx, server1.nodeLease)
	if err != nil {
		t.Fatalf("Failed to check server1 lease: %v", err)
	}
	if ttl1.TTL <= 0 {
		t.Fatalf("Server1 lease is invalid")
	}

	ttl2, err := etcdClient.TimeToLive(ctx, server2.nodeLease)
	if err != nil {
		t.Fatalf("Failed to check server2 lease: %v", err)
	}
	if ttl2.TTL <= 0 {
		t.Fatalf("Server2 lease is invalid")
	}

	t.Logf("✅ Multiple servers acquired independent leases: server1=%d, server2=%d", server1.nodeLease, server2.nodeLease)
}
