package metadata

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockStore is a mock implementation of the Store interface for testing
type mockStore struct {
	routeTable      *RouteTable
	nodes           map[string]*NodeInfo
	migrationStates map[uint32]*MigrationInfo
	mu              sync.RWMutex
}

func newMockStore() *mockStore {
	return &mockStore{
		routeTable:      NewRouteTable(),
		nodes:           make(map[string]*NodeInfo),
		migrationStates: make(map[uint32]*MigrationInfo),
	}
}

func (m *mockStore) RegisterNode(ctx context.Context, node *NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node
	return nil
}

func (m *mockStore) UnregisterNode(ctx context.Context, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
	return nil
}

func (m *mockStore) GetNode(ctx context.Context, nodeID string) (*NodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes[nodeID], nil
}

func (m *mockStore) ListNodes(ctx context.Context, role NodeRole) ([]*NodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*NodeInfo
	for _, node := range m.nodes {
		if node.Role == role {
			result = append(result, node)
		}
	}
	return result, nil
}

func (m *mockStore) UpdateNodeHeartbeat(ctx context.Context, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if node, ok := m.nodes[nodeID]; ok {
		node.LastHeartbeat = time.Now()
	}
	return nil
}

func (m *mockStore) GetRouteTable(ctx context.Context) (*RouteTable, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.routeTable, nil
}

func (m *mockStore) UpdateRouteTable(ctx context.Context, table *RouteTable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routeTable = table
	return nil
}

func (m *mockStore) GetPartition(ctx context.Context, partitionID uint32) (*PartitionInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.routeTable.Partitions[partitionID], nil
}

func (m *mockStore) UpdatePartition(ctx context.Context, partition *PartitionInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routeTable.Partitions[partition.ID] = partition
	return nil
}

func (m *mockStore) WatchRouteTable(ctx context.Context) (<-chan *RouteTable, error) {
	return make(chan *RouteTable), nil
}

func (m *mockStore) WatchNodes(ctx context.Context) (<-chan *NodeEvent, error) {
	return make(chan *NodeEvent), nil
}

func (m *mockStore) Close() error {
	return nil
}

func (m *mockStore) SetMigrationState(ctx context.Context, partitionID uint32, state *MigrationInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrationStates[partitionID] = state
	return nil
}

func (m *mockStore) GetMigrationState(ctx context.Context, partitionID uint32) (*MigrationInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.migrationStates[partitionID], nil
}

func (m *mockStore) DeleteMigrationState(ctx context.Context, partitionID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.migrationStates, partitionID)
	return nil
}

func TestConstants(t *testing.T) {
	if TotalPartitions != 4096 {
		t.Errorf("TotalPartitions = %d, want 4096", TotalPartitions)
	}

	if MinReplicaNodes != 2 {
		t.Errorf("MinReplicaNodes = %d, want 2", MinReplicaNodes)
	}
}

func TestNewRouter(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)

	if router == nil {
		t.Fatal("NewRouter returned nil")
	}

	if router.store != store {
		t.Error("store not set correctly")
	}

	if router.routeTable == nil {
		t.Error("routeTable should not be nil")
	}

	if router.subscribers == nil {
		t.Error("subscribers should not be nil")
	}

	if router.logger == nil {
		t.Error("logger should not be nil")
	}
}

func TestGetRouteTable(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)

	table := router.GetRouteTable()
	if table == nil {
		t.Fatal("GetRouteTable returned nil")
	}

	if table.Version != 1 {
		t.Errorf("Version = %d, want 1", table.Version)
	}
}

func TestGetPartitionNodes(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)

	// Add a partition to the route table
	router.routeTable.Partitions[100] = &PartitionInfo{
		ID:      100,
		Primary: "node-1",
		Replica: "node-2",
		Status:  PartitionStatusNormal,
	}

	primary, replica, err := router.GetPartitionNodes(100)
	if err != nil {
		t.Fatalf("GetPartitionNodes failed: %v", err)
	}

	if primary != "node-1" {
		t.Errorf("Primary = %s, want node-1", primary)
	}

	if replica != "node-2" {
		t.Errorf("Replica = %s, want node-2", replica)
	}
}

func TestGetPartitionNodesNotFound(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)

	_, _, err := router.GetPartitionNodes(9999)
	if err == nil {
		t.Error("expected error for non-existent partition")
	}
}

func TestSubscribeUnsubscribe(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)

	// Subscribe
	ch := router.Subscribe("test-node")
	if ch == nil {
		t.Fatal("Subscribe returned nil channel")
	}

	// Should receive current route table
	select {
	case table := <-ch:
		if table == nil {
			t.Error("received nil table")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for initial route table")
	}

	// Unsubscribe
	router.Unsubscribe("test-node")

	// Channel should be closed
	_, ok := <-ch
	if ok {
		t.Error("channel should be closed after unsubscribe")
	}
}

func TestSubscribeMultiple(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)

	ch1 := router.Subscribe("node-1")
	ch2 := router.Subscribe("node-2")

	if ch1 == nil || ch2 == nil {
		t.Fatal("Subscribe returned nil channel")
	}

	// Drain initial messages
	<-ch1
	<-ch2

	router.Unsubscribe("node-1")
	router.Unsubscribe("node-2")
}

func TestInitializePartitions(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)
	ctx := context.Background()

	// Add storage nodes
	store.RegisterNode(ctx, &NodeInfo{ID: "node-1", Addr: "localhost:8081", Role: NodeRoleStorage})
	store.RegisterNode(ctx, &NodeInfo{ID: "node-2", Addr: "localhost:8082", Role: NodeRoleStorage})

	err := router.InitializePartitions(ctx)
	if err != nil {
		t.Fatalf("InitializePartitions failed: %v", err)
	}

	table := router.GetRouteTable()
	if len(table.Partitions) != TotalPartitions {
		t.Errorf("Partitions count = %d, want %d", len(table.Partitions), TotalPartitions)
	}

	// Verify each partition has valid primary and replica
	for i := uint32(0); i < TotalPartitions; i++ {
		partition, ok := table.Partitions[i]
		if !ok {
			t.Errorf("Partition %d not found", i)
			continue
		}

		if partition.Primary == "" {
			t.Errorf("Partition %d has empty primary", i)
		}

		if partition.Replica == "" {
			t.Errorf("Partition %d has empty replica", i)
		}

		if partition.Primary == partition.Replica {
			t.Errorf("Partition %d has same primary and replica: %s", i, partition.Primary)
		}

		if partition.Status != PartitionStatusNormal {
			t.Errorf("Partition %d status = %s, want normal", i, partition.Status)
		}
	}
}

func TestInitializePartitionsNotEnoughNodes(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)
	ctx := context.Background()

	// Add only one storage node (need at least 2)
	store.RegisterNode(ctx, &NodeInfo{ID: "node-1", Addr: "localhost:8081", Role: NodeRoleStorage})

	err := router.InitializePartitions(ctx)
	if err == nil {
		t.Error("expected error with insufficient nodes")
	}
}

func TestConcurrentGetRouteTable(t *testing.T) {
	store := newMockStore()
	router := NewRouter(store)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			table := router.GetRouteTable()
			if table == nil {
				t.Error("GetRouteTable returned nil")
			}
		}()
	}
	wg.Wait()
}

func BenchmarkGetRouteTable(b *testing.B) {
	store := newMockStore()
	router := NewRouter(store)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = router.GetRouteTable()
	}
}

func BenchmarkGetPartitionNodes(b *testing.B) {
	store := newMockStore()
	router := NewRouter(store)

	// Setup partition
	router.routeTable.Partitions[100] = &PartitionInfo{
		ID:      100,
		Primary: "node-1",
		Replica: "node-2",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = router.GetPartitionNodes(100)
	}
}
