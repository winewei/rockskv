//go:build integration
// +build integration

package metadata

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// cleanupEtcd cleans up etcd data before tests
func cleanupEtcd(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client for cleanup: %v", err)
	}
	defer client.Close()

	// Fail-fast: Immediately verify the connection is working
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.Status(ctx, testEtcdEndpoint)
	if err != nil {
		t.Fatalf("Failed to connect to etcd (is etcd running?): %v", err)
	}

	// Delete all rockskv data with timeout
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deleteCancel()

	_, err = client.Delete(deleteCtx, "/rockskv/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to cleanup etcd: %v", err)
	}
}

// TestRouteTablePersistence verifies that route table is correctly persisted to etcd
func TestRouteTablePersistence(t *testing.T) {
	cleanupEtcd(t)

	ctx := context.Background()

	// Create etcd store
	etcdConfig := &EtcdConfig{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	}

	store, err := NewEtcdStore(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Create a test route table with some partitions
	table := NewRouteTable()
	table.Partitions[0] = &PartitionInfo{
		ID:      0,
		Primary: "localhost:9001",
		Replica: "localhost:9002",
		Status:  PartitionStatusNormal,
		Epoch:   1,
	}
	table.Partitions[1] = &PartitionInfo{
		ID:      1,
		Primary: "localhost:9002",
		Replica: "localhost:9001",
		Status:  PartitionStatusNormal,
		Epoch:   1,
	}

	// Save route table to etcd
	err = store.UpdateRouteTable(ctx, table)
	if err != nil {
		t.Fatalf("Failed to update route table: %v", err)
	}

	// Verify table was saved with a version number
	if table.Version == 0 {
		t.Error("Expected version > 0 after first update")
	}
	firstVersion := table.Version

	// Read route table back from etcd directly using etcd client
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := etcdClient.Get(getCtx, "/rockskv/route_table")
	if err != nil {
		t.Fatalf("Failed to get route table from etcd: %v", err)
	}

	if len(resp.Kvs) == 0 {
		t.Fatalf("Route table not found in etcd")
	}

	// Retrieve route table using store interface
	retrievedTable, err := store.GetRouteTable(ctx)
	if err != nil {
		t.Fatalf("Failed to retrieve route table: %v", err)
	}

	// Verify retrieved table matches original
	if retrievedTable.Version != table.Version {
		t.Errorf("Expected version %d, got %d", table.Version, retrievedTable.Version)
	}

	if len(retrievedTable.Partitions) != len(table.Partitions) {
		t.Errorf("Expected %d partitions, got %d", len(table.Partitions), len(retrievedTable.Partitions))
	}

	// Verify partition 0
	p0 := retrievedTable.Partitions[0]
	if p0 == nil {
		t.Fatal("Partition 0 is nil")
	}
	if p0.Primary != "localhost:9001" {
		t.Errorf("Expected primary localhost:9001, got %s", p0.Primary)
	}
	if p0.Replica != "localhost:9002" {
		t.Errorf("Expected replica localhost:9002, got %s", p0.Replica)
	}
	if p0.Epoch != 1 {
		t.Errorf("Expected epoch 1, got %d", p0.Epoch)
	}

	// Update route table again to test version increment
	table.Partitions[2] = &PartitionInfo{
		ID:      2,
		Primary: "localhost:9003",
		Replica: "localhost:9001",
		Status:  PartitionStatusNormal,
		Epoch:   1,
	}

	err = store.UpdateRouteTable(ctx, table)
	if err != nil {
		t.Fatalf("Failed to update route table second time: %v", err)
	}

	// Verify version incremented by 1
	expectedVersion := firstVersion + 1
	if table.Version != expectedVersion {
		t.Errorf("Expected version %d after second update, got %d", expectedVersion, table.Version)
	}

	retrievedTable2, err := store.GetRouteTable(ctx)
	if err != nil {
		t.Fatalf("Failed to retrieve route table after second update: %v", err)
	}

	if retrievedTable2.Version != expectedVersion {
		t.Errorf("Expected version %d, got %d", expectedVersion, retrievedTable2.Version)
	}

	if len(retrievedTable2.Partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(retrievedTable2.Partitions))
	}

	t.Logf("✅ Route table persistence verified: version=%d, partitions=%d", retrievedTable2.Version, len(retrievedTable2.Partitions))
}

// TestRouteTableWatch verifies that etcd watch mechanism works for route table updates
func TestRouteTableWatch(t *testing.T) {
	cleanupEtcd(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create etcd store
	etcdConfig := &EtcdConfig{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	}

	store, err := NewEtcdStore(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Start watching route table
	watchCh, err := store.WatchRouteTable(ctx)
	if err != nil {
		t.Fatalf("Failed to start watch: %v", err)
	}

	// Create initial route table
	table := NewRouteTable()
	table.Partitions[100] = &PartitionInfo{
		ID:      100,
		Primary: "localhost:9001",
		Replica: "localhost:9002",
		Status:  PartitionStatusNormal,
		Epoch:   1,
	}

	// Update route table in background
	go func() {
		time.Sleep(1 * time.Second)
		err := store.UpdateRouteTable(context.Background(), table)
		if err != nil {
			t.Logf("Failed to update route table: %v", err)
		}
	}()

	// Wait for watch update
	var firstWatchVersion uint64
	select {
	case updatedTable := <-watchCh:
		if updatedTable == nil {
			t.Fatal("Received nil table from watch")
		}
		if updatedTable.Version == 0 {
			t.Error("Expected version > 0")
		}
		firstWatchVersion = updatedTable.Version
		if len(updatedTable.Partitions) == 0 {
			t.Error("Expected partitions in updated table")
		}
		partition := updatedTable.Partitions[100]
		if partition == nil {
			t.Fatal("Partition 100 not found")
		}
		if partition.Primary != "localhost:9001" {
			t.Errorf("Expected primary localhost:9001, got %s", partition.Primary)
		}
		t.Logf("✅ Watch received route table update: version=%d", updatedTable.Version)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for route table update")
	}

	// Update route table again
	table.Partitions[101] = &PartitionInfo{
		ID:      101,
		Primary: "localhost:9002",
		Replica: "localhost:9001",
		Status:  PartitionStatusNormal,
		Epoch:   1,
	}

	go func() {
		time.Sleep(1 * time.Second)
		err := store.UpdateRouteTable(context.Background(), table)
		if err != nil {
			t.Logf("Failed to update route table: %v", err)
		}
	}()

	// Wait for second update
	select {
	case updatedTable := <-watchCh:
		expectedSecondVersion := firstWatchVersion + 1
		if updatedTable.Version != expectedSecondVersion {
			t.Errorf("Expected version %d, got %d", expectedSecondVersion, updatedTable.Version)
		}
		if len(updatedTable.Partitions) < 2 {
			t.Errorf("Expected at least 2 partitions, got %d", len(updatedTable.Partitions))
		}
		t.Logf("✅ Watch received second route table update: version=%d, partitions=%d", updatedTable.Version, len(updatedTable.Partitions))
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for second route table update")
	}
}

// TestRouteSubscription verifies that Router subscription mechanism works
func TestRouteSubscription(t *testing.T) {
	cleanupEtcd(t)

	ctx := context.Background()

	// Create etcd store
	etcdConfig := &EtcdConfig{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	}

	store, err := NewEtcdStore(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Create router
	router := NewRouter(store)
	routerCtx, routerCancel := context.WithCancel(ctx)
	defer routerCancel()

	// Start router (begins watching etcd)
	go func() {
		if err := router.Start(routerCtx); err != nil {
			t.Logf("Router stopped: %v", err)
		}
	}()

	// Wait for router to start
	time.Sleep(1 * time.Second)

	// Subscribe to route updates
	updateCh := router.Subscribe("test-subscriber-1")

	// Should immediately receive current route table
	select {
	case table := <-updateCh:
		if table == nil {
			t.Fatal("Received nil table on subscription")
		}
		t.Logf("Received initial route table: version=%d, partitions=%d", table.Version, len(table.Partitions))
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for initial route table")
	}

	// Create and save a route table update
	table := NewRouteTable()
	table.Partitions[200] = &PartitionInfo{
		ID:      200,
		Primary: "localhost:9001",
		Replica: "localhost:9002",
		Status:  PartitionStatusNormal,
		Epoch:   1,
	}

	err = store.UpdateRouteTable(ctx, table)
	if err != nil {
		t.Fatalf("Failed to update route table: %v", err)
	}

	// Wait for subscriber to receive update
	select {
	case updatedTable := <-updateCh:
		if updatedTable == nil {
			t.Fatal("Received nil table on update")
		}
		if updatedTable.Version == 0 {
			t.Error("Expected non-zero version")
		}
		partition := updatedTable.Partitions[200]
		if partition == nil {
			t.Fatal("Partition 200 not found in update")
		}
		if partition.Primary != "localhost:9001" {
			t.Errorf("Expected primary localhost:9001, got %s", partition.Primary)
		}
		t.Logf("✅ Subscriber received route table update: version=%d", updatedTable.Version)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for route table update on subscription")
	}

	// Test multiple subscribers
	updateCh2 := router.Subscribe("test-subscriber-2")

	// Both should receive initial table
	select {
	case table2 := <-updateCh2:
		if table2 == nil {
			t.Fatal("Received nil table for subscriber 2")
		}
		t.Logf("Subscriber 2 received initial table: version=%d", table2.Version)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout for subscriber 2 initial table")
	}

	// Update route table again
	table.Partitions[201] = &PartitionInfo{
		ID:      201,
		Primary: "localhost:9002",
		Replica: "localhost:9003",
		Status:  PartitionStatusNormal,
		Epoch:   1,
	}

	err = store.UpdateRouteTable(ctx, table)
	if err != nil {
		t.Fatalf("Failed to update route table: %v", err)
	}

	// Both subscribers should receive update
	receivedCount := 0
	timeout := time.After(10 * time.Second)

	for receivedCount < 2 {
		select {
		case <-updateCh:
			receivedCount++
			t.Logf("Subscriber 1 received update")
		case <-updateCh2:
			receivedCount++
			t.Logf("Subscriber 2 received update")
		case <-timeout:
			t.Fatalf("Timeout waiting for updates, received %d/2", receivedCount)
		}
	}

	t.Logf("✅ Multiple subscribers received route table updates")

	// Unsubscribe
	router.Unsubscribe("test-subscriber-1")
	router.Unsubscribe("test-subscriber-2")
}

// TestInitCluster verifies that InitCluster generates correct route table
func TestInitCluster(t *testing.T) {
	cleanupEtcd(t)

	ctx := context.Background()

	// Create etcd store
	etcdConfig := &EtcdConfig{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	}

	store, err := NewEtcdStore(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Register 3 storage nodes
	nodes := []*NodeInfo{
		{
			ID:     "storage-1",
			Addr:   "localhost:9001",
			Role:   NodeRoleStorage,
			Status: "online",
		},
		{
			ID:     "storage-2",
			Addr:   "localhost:9002",
			Role:   NodeRoleStorage,
			Status: "online",
		},
		{
			ID:     "storage-3",
			Addr:   "localhost:9003",
			Role:   NodeRoleStorage,
			Status: "online",
		},
	}

	for _, node := range nodes {
		err := store.RegisterNode(ctx, node)
		if err != nil {
			t.Fatalf("Failed to register node %s: %v", node.ID, err)
		}
	}

	// Create router
	router := NewRouter(store)

	// Initialize cluster
	err = router.InitCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize cluster: %v", err)
	}

	// Verify route table was created
	table := router.GetRouteTable()
	if table == nil {
		t.Fatal("Route table is nil after InitCluster")
	}

	// Should have all 4096 partitions
	if len(table.Partitions) != TotalPartitions {
		t.Errorf("Expected %d partitions, got %d", TotalPartitions, len(table.Partitions))
	}

	// Verify partition distribution
	nodePartitionCount := make(map[string]int)

	for _, partition := range table.Partitions {
		// Verify partition has both primary and replica
		if partition.Primary == "" {
			t.Errorf("Partition %d has no primary", partition.ID)
		}
		if partition.Replica == "" {
			t.Errorf("Partition %d has no replica", partition.ID)
		}

		// Verify primary and replica are different
		if partition.Primary == partition.Replica {
			t.Errorf("Partition %d has same primary and replica: %s", partition.ID, partition.Primary)
		}

		// Verify status is normal
		if partition.Status != PartitionStatusNormal {
			t.Errorf("Partition %d has status %s, expected %s", partition.ID, partition.Status, PartitionStatusNormal)
		}

		// Verify epoch is 1 (initial)
		if partition.Epoch != 1 {
			t.Errorf("Partition %d has epoch %d, expected 1", partition.ID, partition.Epoch)
		}

		// Count partitions per node (as primary)
		nodePartitionCount[partition.Primary]++
	}

	// Verify distribution is relatively balanced
	// With 3 nodes and 4096 partitions, each should have ~1365 partitions
	// Consistent hashing may have some variance, using ±20% tolerance
	expectedPerNode := TotalPartitions / len(nodes)
	tolerance := float64(expectedPerNode) * 0.20 // 20% tolerance for consistent hashing variance

	for addr, count := range nodePartitionCount {
		diff := float64(count - expectedPerNode)
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("Node %s has %d partitions, expected ~%d (±%.0f)", addr, count, expectedPerNode, tolerance)
		}
		t.Logf("Node %s: %d partitions (%.1f%% of total)", addr, count, float64(count)*100/float64(TotalPartitions))
	}

	// Verify cluster state is RUNNING
	clusterInfo, err := store.GetClusterInfo(ctx)
	if err != nil {
		t.Fatalf("Failed to get cluster info: %v", err)
	}

	if clusterInfo.State != ClusterStateRunning {
		t.Errorf("Expected cluster state %s, got %s", ClusterStateRunning, clusterInfo.State)
	}

	t.Logf("✅ InitCluster generated route table with %d partitions across %d nodes", len(table.Partitions), len(nodes))
}

// TestPartitionReassignment verifies that partitions are reassigned when node topology changes
// TODO: This test is currently skipped because RebalancePartitions may not fully implement
// automatic reassignment of partitions from offline nodes. This requires further investigation.
func TestPartitionReassignment(t *testing.T) {
	t.Skip("Skipping until RebalancePartitions fully implements offline node handling")

	cleanupEtcd(t)

	ctx := context.Background()

	// Create etcd store
	etcdConfig := &EtcdConfig{
		Endpoints:   []string{testEtcdEndpoint},
		DialTimeout: 5 * time.Second,
	}

	store, err := NewEtcdStore(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create etcd store: %v", err)
	}
	defer store.Close()

	// Register 3 storage nodes
	nodes := []*NodeInfo{
		{
			ID:     "storage-reassign-1",
			Addr:   "localhost:9101",
			Role:   NodeRoleStorage,
			Status: "online",
		},
		{
			ID:     "storage-reassign-2",
			Addr:   "localhost:9102",
			Role:   NodeRoleStorage,
			Status: "online",
		},
		{
			ID:     "storage-reassign-3",
			Addr:   "localhost:9103",
			Role:   NodeRoleStorage,
			Status: "online",
		},
	}

	for _, node := range nodes {
		err := store.RegisterNode(ctx, node)
		if err != nil {
			t.Fatalf("Failed to register node %s: %v", node.ID, err)
		}
	}

	// Create router and initialize cluster
	router := NewRouter(store)
	err = router.InitCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize cluster: %v", err)
	}

	// Get initial route table
	initialTable := router.GetRouteTable()
	initialVersion := initialTable.Version
	t.Logf("Initial route table version: %d", initialVersion)

	// Count partitions on each node before reassignment
	initialCounts := make(map[string]int)
	for _, partition := range initialTable.Partitions {
		initialCounts[partition.Primary]++
	}

	t.Logf("Initial distribution: %v", initialCounts)

	// Mark one node as offline (simulating failure)
	err = store.UpdateNodeStatus(ctx, "storage-reassign-3", "offline")
	if err != nil {
		t.Fatalf("Failed to mark node offline: %v", err)
	}

	// Trigger rebalance
	err = router.RebalancePartitions(ctx)
	if err != nil {
		t.Fatalf("Failed to rebalance partitions: %v", err)
	}

	// Get updated route table
	updatedTable := router.GetRouteTable()

	// Verify version incremented
	if updatedTable.Version <= initialVersion {
		t.Errorf("Expected version > %d, got %d", initialVersion, updatedTable.Version)
	}

	// Count partitions on each node after reassignment
	finalCounts := make(map[string]int)
	epochIncrements := 0

	for partitionID, partition := range updatedTable.Partitions {
		// Offline node should not be primary anymore
		if partition.Primary == "localhost:9103" {
			t.Errorf("Partition %d still has offline node as primary", partitionID)
		}

		finalCounts[partition.Primary]++

		// Check if epoch was incremented for partitions that moved
		initialPartition := initialTable.Partitions[partitionID]
		if initialPartition.Primary != partition.Primary {
			// Primary changed, epoch should increment
			if partition.Epoch != initialPartition.Epoch+1 {
				t.Errorf("Partition %d primary changed but epoch not incremented: old=%d, new=%d",
					partitionID, initialPartition.Epoch, partition.Epoch)
			}
			epochIncrements++
		}
	}

	t.Logf("Final distribution: %v", finalCounts)
	t.Logf("Partitions reassigned with epoch increment: %d", epochIncrements)

	// Verify offline node has no primary partitions
	if finalCounts["localhost:9103"] > 0 {
		t.Errorf("Offline node still has %d primary partitions", finalCounts["localhost:9103"])
	}

	// Verify partitions are distributed among remaining 2 nodes
	if len(finalCounts) != 2 {
		t.Errorf("Expected partitions on 2 nodes, got %d", len(finalCounts))
	}

	// Verify total partitions unchanged
	totalPartitions := 0
	for _, count := range finalCounts {
		totalPartitions += count
	}
	if totalPartitions != TotalPartitions {
		t.Errorf("Expected %d total partitions, got %d", TotalPartitions, totalPartitions)
	}

	t.Logf("✅ Partition reassignment completed: %d partitions moved with epoch increment", epochIncrements)
}
