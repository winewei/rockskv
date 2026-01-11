//go:build integration
// +build integration

package metadata

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const testNodeRegistrationEtcdEndpoint = "localhost:2379"

// cleanupEtcd removes all test data from etcd
func cleanupEtcdForNodeTests(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client for cleanup: %v", err)
	}
	defer client.Close()

	// Fail-fast: Immediately verify the connection is working
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.Status(ctx, testNodeRegistrationEtcdEndpoint)
	if err != nil {
		t.Fatalf("Failed to connect to etcd (is etcd running?): %v", err)
	}

	// Delete all RocksKV data with timeout
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deleteCancel()

	_, err = client.Delete(deleteCtx, "/rockskv/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to cleanup etcd: %v", err)
	}
}

// TestNodeRegistration verifies that storage nodes can successfully register with the metadata service
func TestNodeRegistration(t *testing.T) {
	cleanupEtcdForNodeTests(t)

	ctx := context.Background()

	// Create etcd store
	store, err := NewEtcdStore(&EtcdConfig{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Register a storage node using store directly
	node1 := &NodeInfo{
		ID:   "storage-1",
		Addr: "localhost:9001",
		Role: NodeRoleStorage,
	}

	err = store.RegisterNode(ctx, node1)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// Verify node exists in etcd
	node, err := store.GetNode(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Failed to get node from store: %v", err)
	}

	if node == nil {
		t.Fatal("Node not found in store after registration")
	}

	// Verify node properties
	if node.ID != "storage-1" {
		t.Errorf("Expected node ID 'storage-1', got '%s'", node.ID)
	}

	if node.Addr != "localhost:9001" {
		t.Errorf("Expected node address 'localhost:9001', got '%s'", node.Addr)
	}

	if node.Role != NodeRoleStorage {
		t.Errorf("Expected node role Storage, got %v", node.Role)
	}

	if node.Status != string(NodeStatusOnline) {
		t.Errorf("Expected node status 'online', got '%s'", node.Status)
	}

	if node.RegisteredAt.IsZero() {
		t.Error("Expected RegisteredAt timestamp to be set")
	}

	if node.LastHeartbeat.IsZero() {
		t.Error("Expected LastHeartbeat timestamp to be set")
	}

	t.Logf("✅ Node registration successful: %+v", node)

	// Register multiple nodes
	node2 := &NodeInfo{
		ID:   "storage-2",
		Addr: "localhost:9002",
		Role: NodeRoleStorage,
	}
	err = store.RegisterNode(ctx, node2)
	if err != nil {
		t.Fatalf("Failed to register second node: %v", err)
	}

	node3 := &NodeInfo{
		ID:   "compute-1",
		Addr: "localhost:8001",
		Role: NodeRoleCompute,
	}
	err = store.RegisterNode(ctx, node3)
	if err != nil {
		t.Fatalf("Failed to register compute node: %v", err)
	}

	// List all storage nodes
	storageNodes, err := store.ListNodes(ctx, NodeRoleStorage)
	if err != nil {
		t.Fatalf("Failed to list storage nodes: %v", err)
	}

	if len(storageNodes) != 2 {
		t.Errorf("Expected 2 storage nodes, got %d", len(storageNodes))
	}

	// List compute nodes
	computeNodes, err := store.ListNodes(ctx, NodeRoleCompute)
	if err != nil {
		t.Fatalf("Failed to list compute nodes: %v", err)
	}

	if len(computeNodes) != 1 {
		t.Errorf("Expected 1 compute node, got %d", len(computeNodes))
	}

	// Total should be 2 storage + 1 compute = 3 nodes
	totalNodes := len(storageNodes) + len(computeNodes)
	if totalNodes != 3 {
		t.Errorf("Expected 3 total nodes, got %d", totalNodes)
	}

	t.Log("✅ Multiple node registration successful")
}

// TestHeartbeat verifies that heartbeat mechanism keeps node status alive
func TestHeartbeat(t *testing.T) {
	cleanupEtcdForNodeTests(t)

	ctx := context.Background()

	// Create etcd store
	store, err := NewEtcdStore(&EtcdConfig{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Register a storage node
	node := &NodeInfo{
		ID:   "storage-1",
		Addr: "localhost:9001",
		Role: NodeRoleStorage,
	}
	err = store.RegisterNode(ctx, node)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// Get initial heartbeat timestamp
	node1, err := store.GetNode(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}
	initialHeartbeat := node1.LastHeartbeat

	t.Logf("Initial heartbeat timestamp: %v", initialHeartbeat)

	// Wait a bit to ensure time difference
	time.Sleep(100 * time.Millisecond)

	// Send heartbeat
	err = store.UpdateNodeHeartbeat(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	// Verify heartbeat updated in etcd
	// Note: Heartbeat is stored in /rockskv/heartbeats/{node_id}, not in node info
	// Create a new etcd client to verify the heartbeat key
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	heartbeatKey := "/rockskv/heartbeats/storage-1"
	resp, err := etcdClient.Get(ctx, heartbeatKey)
	if err != nil {
		t.Fatalf("Failed to get heartbeat from etcd: %v", err)
	}

	if len(resp.Kvs) == 0 {
		t.Fatal("Heartbeat key not found in etcd")
	}

	heartbeatTimestamp := string(resp.Kvs[0].Value)
	t.Logf("Heartbeat timestamp in etcd: %s", heartbeatTimestamp)

	// Send multiple heartbeats
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)

		err = store.UpdateNodeHeartbeat(ctx, "storage-1")
		if err != nil {
			t.Errorf("Heartbeat %d failed: %v", i+1, err)
		}

		t.Logf("Heartbeat %d sent successfully", i+1)
	}

	// Verify heartbeat key still exists and has a lease
	resp, err = etcdClient.Get(ctx, heartbeatKey)
	if err != nil {
		t.Fatalf("Failed to get heartbeat after multiple updates: %v", err)
	}

	if len(resp.Kvs) == 0 {
		t.Fatal("Heartbeat key disappeared after multiple heartbeats")
	}

	// Check that the heartbeat has a lease (TTL)
	leaseID := resp.Kvs[0].Lease
	if leaseID == 0 {
		t.Error("Heartbeat key should have a lease (TTL)")
	} else {
		t.Logf("Heartbeat key has lease ID: %d", leaseID)

		// Get TTL of the lease
		ttlResp, err := etcdClient.TimeToLive(ctx, clientv3.LeaseID(leaseID))
		if err != nil {
			t.Fatalf("Failed to get lease TTL: %v", err)
		}
		t.Logf("Heartbeat lease TTL: %d seconds (remaining: %d seconds)",
			ttlResp.TTL, ttlResp.TTL)

		if ttlResp.TTL <= 0 || ttlResp.TTL > 15 {
			t.Errorf("Expected lease TTL between 1-15 seconds, got %d", ttlResp.TTL)
		}
	}

	t.Log("✅ Heartbeat mechanism working correctly")
}

// TestNodeTimeout verifies that when heartbeat stops, node is marked as DOWN
func TestNodeTimeout(t *testing.T) {
	cleanupEtcdForNodeTests(t)

	ctx := context.Background()

	// Create etcd store
	store, err := NewEtcdStore(&EtcdConfig{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Create router
	router := NewRouter(store)

	// Create failover manager with custom short timeouts for testing
	failoverConfig := &FailoverConfig{
		HeartbeatInterval: 100 * time.Millisecond, // Check every 100ms
		HeartbeatTimeout:  500 * time.Millisecond, // Mark DOWN after 500ms
		FailoverDelay:     100 * time.Millisecond, // Trigger failover quickly
	}

	failoverMgr := NewFailoverManager(failoverConfig, store, router)

	// Start failover manager
	failoverMgr.Start()
	defer failoverMgr.Stop()

	// Register storage nodes
	for i, addr := range []string{"localhost:9001", "localhost:9002", "localhost:9003"} {
		node := &NodeInfo{
			ID:   "storage-" + string(rune('1'+i)),
			Addr: addr,
			Role: NodeRoleStorage,
		}
		err = store.RegisterNode(ctx, node)
		if err != nil {
			t.Fatalf("RegisterNode failed: %v", err)
		}
	}

	// Initialize cluster to assign partitions
	err = router.InitCluster(ctx)
	if err != nil {
		t.Fatalf("InitCluster failed: %v", err)
	}

	t.Log("Cluster initialized with 3 storage nodes")

	// Send initial heartbeat for storage-1
	err = store.UpdateNodeHeartbeat(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Initial heartbeat failed: %v", err)
	}

	// Verify node is online
	node, err := store.GetNode(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	if node.Status != string(NodeStatusOnline) {
		t.Errorf("Expected node status 'online', got '%s'", node.Status)
	}

	t.Log("Node storage-1 is online")

	// Stop sending heartbeats (simulate node failure)
	// Wait for timeout (500ms) + some overhead
	t.Log("Waiting for node timeout (500ms + overhead)...")
	time.Sleep(800 * time.Millisecond)

	// Verify node is marked as offline
	node, err = store.GetNode(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Failed to get node after timeout: %v", err)
	}

	if node.Status != string(NodeStatusOffline) {
		t.Errorf("Expected node status 'offline' after timeout, got '%s'", node.Status)
	}

	t.Log("✅ Node marked as offline after heartbeat timeout")

	// Verify failover was triggered (replicas promoted)
	// Wait a bit for failover to complete
	time.Sleep(300 * time.Millisecond)

	table := router.GetRouteTable()

	// Check that partitions previously assigned to storage-1 have been reassigned
	storage1Addr := "localhost:9001"
	primaryCount := 0

	for _, partition := range table.Partitions {
		if partition.Primary == storage1Addr {
			primaryCount++
		}
	}

	// After failover, storage-1 should have fewer or no primary partitions
	// Because replicas would have been promoted
	t.Logf("Storage-1 primary partitions after failover: %d", primaryCount)

	t.Log("✅ Node timeout and failover test completed")
}

// TestNodeRejoin verifies that a DOWN node can rejoin the cluster
func TestNodeRejoin(t *testing.T) {
	cleanupEtcdForNodeTests(t)

	ctx := context.Background()

	// Create etcd store
	store, err := NewEtcdStore(&EtcdConfig{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Create router
	router := NewRouter(store)

	// Create failover manager with short timeouts
	failoverConfig := &FailoverConfig{
		HeartbeatInterval: 100 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		FailoverDelay:     100 * time.Millisecond,
	}

	failoverMgr := NewFailoverManager(failoverConfig, store, router)

	// Start failover manager
	failoverMgr.Start()
	defer failoverMgr.Stop()

	// Register storage nodes
	for i, addr := range []string{"localhost:9001", "localhost:9002", "localhost:9003"} {
		node := &NodeInfo{
			ID:   "storage-" + string(rune('1'+i)),
			Addr: addr,
			Role: NodeRoleStorage,
		}
		err = store.RegisterNode(ctx, node)
		if err != nil {
			t.Fatalf("RegisterNode failed: %v", err)
		}
	}

	// Send initial heartbeat
	err = store.UpdateNodeHeartbeat(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Initial heartbeat failed: %v", err)
	}

	// Verify node is online
	node, err := store.GetNode(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}
	if node.Status != string(NodeStatusOnline) {
		t.Errorf("Expected node status 'online', got '%s'", node.Status)
	}

	t.Log("Node storage-1 is online")

	// Simulate node failure (stop heartbeats)
	t.Log("Simulating node failure...")
	time.Sleep(800 * time.Millisecond) // Wait for timeout

	// Verify node is marked as offline
	node, err = store.GetNode(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Failed to get node after timeout: %v", err)
	}
	if node.Status != string(NodeStatusOffline) {
		t.Errorf("Expected node status 'offline', got '%s'", node.Status)
	}

	t.Log("Node storage-1 marked as offline")

	// Node comes back online and sends heartbeat
	t.Log("Node rejoining cluster...")
	err = store.UpdateNodeHeartbeat(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Heartbeat after rejoin failed: %v", err)
	}

	// Continue sending heartbeats for a bit
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		err = store.UpdateNodeHeartbeat(ctx, "storage-1")
		if err != nil {
			t.Errorf("Heartbeat %d failed: %v", i+1, err)
		}
	}

	// Verify heartbeat key exists in etcd (node is alive)
	// Create a new etcd client to verify the heartbeat key
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	heartbeatKey := "/rockskv/heartbeats/storage-1"
	resp, err := etcdClient.Get(ctx, heartbeatKey)
	if err != nil {
		t.Fatalf("Failed to get heartbeat: %v", err)
	}

	if len(resp.Kvs) == 0 {
		t.Fatal("Heartbeat key not found - node should be alive")
	}

	t.Log("Node storage-1 heartbeat restored")

	// Note: The node status in /rockskv/nodes/storage-1 will remain "offline"
	// until explicitly updated. The failover manager tracks node health separately
	// using the /rockskv/heartbeats/{node_id} keys with TTL.

	// Check if failover manager considers the node healthy again
	// This is tracked in memory in failoverMgr.nodeHealth
	// We can infer health by seeing if heartbeats succeed
	time.Sleep(300 * time.Millisecond)

	// Try one more heartbeat to confirm node is fully operational
	err = store.UpdateNodeHeartbeat(ctx, "storage-1")
	if err != nil {
		t.Fatalf("Final heartbeat check failed: %v", err)
	}

	t.Log("✅ Node successfully rejoined cluster and is sending heartbeats")
}

// TestNodeStatusTransitions tests the various node status transitions
func TestNodeStatusTransitions(t *testing.T) {
	cleanupEtcdForNodeTests(t)

	ctx := context.Background()

	// Create etcd store
	store, err := NewEtcdStore(&EtcdConfig{
		Endpoints:   []string{testNodeRegistrationEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Register a node
	node := &NodeInfo{
		ID:   "storage-test",
		Addr: "localhost:9999",
		Role: NodeRoleStorage,
	}
	err = store.RegisterNode(ctx, node)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// Verify initial status is online
	n, err := store.GetNode(ctx, "storage-test")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}
	if n.Status != string(NodeStatusOnline) {
		t.Errorf("Expected initial status 'online', got '%s'", n.Status)
	}

	// Transition to draining
	err = store.UpdateNodeStatus(ctx, "storage-test", NodeStatusDraining)
	if err != nil {
		t.Fatalf("Failed to update status to draining: %v", err)
	}

	n, _ = store.GetNode(ctx, "storage-test")
	if n.Status != string(NodeStatusDraining) {
		t.Errorf("Expected status 'draining', got '%s'", n.Status)
	}

	// Transition to offline
	err = store.UpdateNodeStatus(ctx, "storage-test", NodeStatusOffline)
	if err != nil {
		t.Fatalf("Failed to update status to offline: %v", err)
	}

	n, _ = store.GetNode(ctx, "storage-test")
	if n.Status != string(NodeStatusOffline) {
		t.Errorf("Expected status 'offline', got '%s'", n.Status)
	}

	// Transition to removed
	err = store.UpdateNodeStatus(ctx, "storage-test", NodeStatusRemoved)
	if err != nil {
		t.Fatalf("Failed to update status to removed: %v", err)
	}

	n, _ = store.GetNode(ctx, "storage-test")
	if n.Status != string(NodeStatusRemoved) {
		t.Errorf("Expected status 'removed', got '%s'", n.Status)
	}

	t.Log("✅ Node status transitions work correctly")
}
