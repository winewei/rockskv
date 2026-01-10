package metadata

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

const (
	// TotalPartitions is the fixed number of partitions
	TotalPartitions = 4096

	// MinReplicaNodes is the minimum number of storage nodes for replication
	MinReplicaNodes = 2
)

// Router manages partition routing
type Router struct {
	store       Store
	routeTable  *RouteTable
	mu          sync.RWMutex
	subscribers map[string]chan *RouteTable
	subMu       sync.RWMutex
	logger      *zap.Logger
}

// NewRouter creates a new router
func NewRouter(store Store) *Router {
	return &Router{
		store:       store,
		routeTable:  NewRouteTable(),
		subscribers: make(map[string]chan *RouteTable),
		logger:      common.NewLogger("router"),
	}
}

// Start starts the router
func (r *Router) Start(ctx context.Context) error {
	// Load initial route table
	table, err := r.store.GetRouteTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to load route table: %w", err)
	}
	r.updateRouteTable(table)

	// Watch for route table changes
	watchCh, err := r.store.WatchRouteTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to watch route table: %w", err)
	}

	go func() {
		for table := range watchCh {
			r.updateRouteTable(table)
			r.notifySubscribers(table)
		}
	}()

	return nil
}

// updateRouteTable updates the local route table
func (r *Router) updateRouteTable(table *RouteTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routeTable = table
	common.RouteTableVersion.Set(float64(table.Version))
	r.logger.Info("Route table updated", zap.Uint64("version", table.Version))
}

// GetRouteTable returns the current route table
func (r *Router) GetRouteTable() *RouteTable {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.routeTable
}

// GetPartitionNodes returns the primary and replica nodes for a partition
func (r *Router) GetPartitionNodes(partitionID uint32) (primary, replica string, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partition, ok := r.routeTable.Partitions[partitionID]
	if !ok {
		return "", "", fmt.Errorf("partition %d not found", partitionID)
	}

	return partition.Primary, partition.Replica, nil
}

// Subscribe adds a subscriber for route table updates
func (r *Router) Subscribe(nodeID string) <-chan *RouteTable {
	r.subMu.Lock()
	defer r.subMu.Unlock()

	ch := make(chan *RouteTable, 10)
	r.subscribers[nodeID] = ch

	// Send current route table
	r.mu.RLock()
	currentTable := r.routeTable
	r.mu.RUnlock()

	select {
	case ch <- currentTable:
	default:
	}

	return ch
}

// Unsubscribe removes a subscriber
func (r *Router) Unsubscribe(nodeID string) {
	r.subMu.Lock()
	defer r.subMu.Unlock()

	if ch, ok := r.subscribers[nodeID]; ok {
		close(ch)
		delete(r.subscribers, nodeID)
	}
}

// notifySubscribers notifies all subscribers of a route table update
func (r *Router) notifySubscribers(table *RouteTable) {
	r.subMu.RLock()
	defer r.subMu.RUnlock()

	for nodeID, ch := range r.subscribers {
		select {
		case ch <- table:
		default:
			r.logger.Warn("Subscriber channel full", zap.String("node_id", nodeID))
		}
	}
}

// GetClusterState returns the current cluster state
func (r *Router) GetClusterState(ctx context.Context) (ClusterState, error) {
	info, err := r.store.GetClusterInfo(ctx)
	if err != nil {
		return "", err
	}
	return info.State, nil
}

// InitCluster initializes the cluster with partition allocation
// This is the two-phase initialization: PENDING -> INITIALIZING -> RUNNING
func (r *Router) InitCluster(ctx context.Context) error {
	// Phase 1: Pre-checks
	info, err := r.store.GetClusterInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %w", err)
	}

	if info.State != ClusterStatePending {
		return fmt.Errorf("cluster is not in PENDING state (current: %s)", info.State)
	}

	nodes, err := r.store.ListNodes(ctx, NodeRoleStorage)
	if err != nil {
		return fmt.Errorf("failed to list storage nodes: %w", err)
	}

	if len(nodes) < MinReplicaNodes {
		return fmt.Errorf("need at least %d storage nodes, got %d", MinReplicaNodes, len(nodes))
	}

	// Check all nodes are online (have recent heartbeat)
	for _, node := range nodes {
		if node.Status != "online" {
			return fmt.Errorf("node %s is not online (status: %s)", node.ID, node.Status)
		}
	}

	// Phase 2: Set state to INITIALIZING
	if err := r.store.SetClusterState(ctx, ClusterStateInitializing); err != nil {
		return fmt.Errorf("failed to set cluster state to INITIALIZING: %w", err)
	}

	r.logger.Info("Cluster initialization started",
		zap.Int("node_count", len(nodes)),
	)

	// Phase 3: Allocate partitions
	if err := r.allocatePartitions(ctx, nodes); err != nil {
		// Rollback to PENDING on failure
		if rollbackErr := r.store.SetClusterState(ctx, ClusterStatePending); rollbackErr != nil {
			r.logger.Error("Failed to rollback cluster state", zap.Error(rollbackErr))
		}
		return fmt.Errorf("failed to allocate partitions: %w", err)
	}

	// Phase 4: Set state to RUNNING
	if err := r.store.SetClusterState(ctx, ClusterStateRunning); err != nil {
		return fmt.Errorf("failed to set cluster state to RUNNING: %w", err)
	}

	r.logger.Info("Cluster initialization completed",
		zap.Int("partition_count", TotalPartitions),
		zap.Int("node_count", len(nodes)),
	)

	return nil
}

// allocatePartitions distributes partitions across storage nodes
func (r *Router) allocatePartitions(ctx context.Context, nodes []*NodeInfo) error {
	// Sort nodes by ID for consistent assignment
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	nodeCount := len(nodes)
	table := NewRouteTable()

	// Distribute partitions across nodes
	for i := uint32(0); i < TotalPartitions; i++ {
		primaryIdx := int(i) % nodeCount
		replicaIdx := (primaryIdx + 1) % nodeCount

		table.Partitions[i] = &PartitionInfo{
			ID:      i,
			Primary: nodes[primaryIdx].Addr,
			Replica: nodes[replicaIdx].Addr,
			Status:  PartitionStatusNormal,
		}
	}

	table.UpdatedAt = time.Now()

	// Save route table
	if err := r.store.UpdateRouteTable(ctx, table); err != nil {
		return fmt.Errorf("failed to save route table: %w", err)
	}

	r.updateRouteTable(table)
	return nil
}

// InitializePartitions initializes partitions across available storage nodes
// Deprecated: Use InitCluster for proper two-phase initialization
func (r *Router) InitializePartitions(ctx context.Context) error {
	nodes, err := r.store.ListNodes(ctx, NodeRoleStorage)
	if err != nil {
		return fmt.Errorf("failed to list storage nodes: %w", err)
	}

	if len(nodes) < MinReplicaNodes {
		return fmt.Errorf("need at least %d storage nodes, got %d", MinReplicaNodes, len(nodes))
	}

	// Sort nodes by ID for consistent assignment
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	nodeCount := len(nodes)
	table := NewRouteTable()

	// Distribute partitions across nodes
	for i := uint32(0); i < TotalPartitions; i++ {
		primaryIdx := int(i) % nodeCount
		replicaIdx := (primaryIdx + 1) % nodeCount

		table.Partitions[i] = &PartitionInfo{
			ID:      i,
			Primary: nodes[primaryIdx].Addr,
			Replica: nodes[replicaIdx].Addr,
			Status:  PartitionStatusNormal,
		}
	}

	table.UpdatedAt = time.Now()

	// Save route table
	if err := r.store.UpdateRouteTable(ctx, table); err != nil {
		return fmt.Errorf("failed to save route table: %w", err)
	}

	r.updateRouteTable(table)

	r.logger.Info("Partitions initialized",
		zap.Int("partition_count", TotalPartitions),
		zap.Int("node_count", nodeCount),
	)

	return nil
}

// RebalancePartitions rebalances partitions after node changes
func (r *Router) RebalancePartitions(ctx context.Context) error {
	nodes, err := r.store.ListNodes(ctx, NodeRoleStorage)
	if err != nil {
		return fmt.Errorf("failed to list storage nodes: %w", err)
	}

	if len(nodes) < MinReplicaNodes {
		return fmt.Errorf("need at least %d storage nodes, got %d", MinReplicaNodes, len(nodes))
	}

	// Sort nodes by ID for consistent assignment
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	r.mu.Lock()
	currentTable := r.routeTable
	r.mu.Unlock()

	// Create node address set for quick lookup
	addrSet := make(map[string]bool)
	for _, node := range nodes {
		addrSet[node.Addr] = true
	}

	newTable := &RouteTable{
		Version:    currentTable.Version,
		Partitions: make(map[uint32]*PartitionInfo),
		UpdatedAt:  time.Now(),
	}

	nodeCount := len(nodes)
	changes := 0

	for partitionID, partition := range currentTable.Partitions {
		newPartition := &PartitionInfo{
			ID:     partition.ID,
			Status: partition.Status,
		}

		// Check if primary is still available
		if addrSet[partition.Primary] {
			newPartition.Primary = partition.Primary
		} else {
			// Assign new primary
			newPartition.Primary = nodes[int(partitionID)%nodeCount].Addr
			changes++
		}

		// Check if replica is still available and different from primary
		if addrSet[partition.Replica] && partition.Replica != newPartition.Primary {
			newPartition.Replica = partition.Replica
		} else {
			// Assign new replica
			replicaIdx := (int(partitionID) + 1) % nodeCount
			if nodes[replicaIdx].Addr == newPartition.Primary {
				replicaIdx = (replicaIdx + 1) % nodeCount
			}
			newPartition.Replica = nodes[replicaIdx].Addr
			changes++
		}

		newTable.Partitions[partitionID] = newPartition
	}

	if changes > 0 {
		if err := r.store.UpdateRouteTable(ctx, newTable); err != nil {
			return fmt.Errorf("failed to save route table: %w", err)
		}

		// Don't manually update - let etcd watch handle it to avoid duplicate notifications

		r.logger.Info("Partitions rebalanced",
			zap.Int("changes", changes),
			zap.Int("node_count", nodeCount),
		)
	}

	return nil
}

// PromoteReplica promotes a replica to primary for a partition
func (r *Router) PromoteReplica(ctx context.Context, partitionID uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	partition, ok := r.routeTable.Partitions[partitionID]
	if !ok {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Swap primary and replica
	newTable := &RouteTable{
		Version:    r.routeTable.Version,
		Partitions: make(map[uint32]*PartitionInfo),
		UpdatedAt:  time.Now(),
	}

	// Copy all partitions
	for id, p := range r.routeTable.Partitions {
		newTable.Partitions[id] = &PartitionInfo{
			ID:      p.ID,
			Primary: p.Primary,
			Replica: p.Replica,
			Status:  p.Status,
		}
	}

	// Swap for the target partition
	newTable.Partitions[partitionID].Primary = partition.Replica
	newTable.Partitions[partitionID].Replica = partition.Primary

	// Save to etcd while holding lock to prevent concurrent modifications
	if err := r.store.UpdateRouteTable(ctx, newTable); err != nil {
		return fmt.Errorf("failed to save route table: %w", err)
	}

	// Don't manually update - let etcd watch handle it to avoid duplicate notifications

	r.logger.Info("Replica promoted to primary",
		zap.Uint32("partition_id", partitionID),
		zap.String("new_primary", newTable.Partitions[partitionID].Primary),
	)

	return nil
}

// SetPartitionStatus updates the status of a partition
func (r *Router) SetPartitionStatus(ctx context.Context, partitionID uint32, status PartitionStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.routeTable.Partitions[partitionID]; !ok {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	newTable := &RouteTable{
		Version:    r.routeTable.Version,
		Partitions: make(map[uint32]*PartitionInfo),
		UpdatedAt:  time.Now(),
	}

	// Copy all partitions
	for id, p := range r.routeTable.Partitions {
		newTable.Partitions[id] = &PartitionInfo{
			ID:              p.ID,
			Primary:         p.Primary,
			Replica:         p.Replica,
			Status:          p.Status,
			MigrationTarget: p.MigrationTarget,
			MigrationState:  p.MigrationState,
		}
	}

	newTable.Partitions[partitionID].Status = status

	// Save to etcd while holding lock to prevent concurrent modifications
	if err := r.store.UpdateRouteTable(ctx, newTable); err != nil {
		return fmt.Errorf("failed to save route table: %w", err)
	}

	// Don't manually update - let etcd watch handle it to avoid duplicate notifications

	return nil
}

// SetPartitionMigration marks a partition as being migrated
func (r *Router) SetPartitionMigration(ctx context.Context, partitionID uint32, targetNode string, state pb.MigrationState) error {
	r.mu.RLock()
	partition, ok := r.routeTable.Partitions[partitionID]
	if !ok {
		r.mu.RUnlock()
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Determine if this is a primary or replica migration
	isPrimaryMove := partition.Primary != targetNode
	r.mu.RUnlock()

	// Store migration state separately (not in route table)
	// This reduces write size from 382KB to ~200B (1900x reduction!)
	migrationInfo := &MigrationInfo{
		PartitionID:   partitionID,
		Target:        targetNode,
		State:         state,
		IsPrimaryMove: isPrimaryMove,
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := r.store.SetMigrationState(ctx, partitionID, migrationInfo); err != nil {
		return err
	}

	r.logger.Info("Partition migration state set",
		zap.Uint32("partition_id", partitionID),
		zap.String("target", targetNode),
		zap.Int32("state", int32(state)),
	)

	return nil
}

// CompleteMigration finalizes a partition migration
func (r *Router) CompleteMigration(ctx context.Context, partitionID uint32, newNode string, isPrimary bool) error {
	// First, delete the migration state (cleanup temporary data)
	if err := r.store.DeleteMigrationState(ctx, partitionID); err != nil {
		r.logger.Warn("Failed to delete migration state (continuing anyway)",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
	}

	// Now update the route table with the new routing (this IS necessary)
	r.mu.Lock()
	defer r.mu.Unlock()

	partition, ok := r.routeTable.Partitions[partitionID]
	if !ok {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	newTable := &RouteTable{
		Version:    r.routeTable.Version,
		Partitions: make(map[uint32]*PartitionInfo),
		UpdatedAt:  time.Now(),
	}

	// Copy all partitions (without migration state fields)
	for id, p := range r.routeTable.Partitions {
		newTable.Partitions[id] = &PartitionInfo{
			ID:      p.ID,
			Primary: p.Primary,
			Replica: p.Replica,
			Status:  p.Status,
			// MigrationTarget and MigrationState no longer stored here
		}
	}

	// Update the migrated partition's actual routing
	if isPrimary {
		newTable.Partitions[partitionID].Primary = newNode
		// Old primary becomes replica if still valid
		if partition.Primary != newNode {
			newTable.Partitions[partitionID].Replica = partition.Primary
		}
	} else {
		newTable.Partitions[partitionID].Replica = newNode
	}
	newTable.Partitions[partitionID].Status = PartitionStatusNormal

	// Save to etcd while holding lock to prevent concurrent modifications
	if err := r.store.UpdateRouteTable(ctx, newTable); err != nil {
		return err
	}

	// Don't manually update - let etcd watch handle it to avoid duplicate notifications

	r.logger.Info("Partition migration completed",
		zap.Uint32("partition_id", partitionID),
		zap.String("new_node", newNode),
		zap.Bool("is_primary", isPrimary),
	)

	return nil
}

// GetPartitionMigrationTarget returns the migration target for a partition
// Now reads from separate migration state storage instead of route table
func (r *Router) GetPartitionMigrationTarget(partitionID uint32) (string, pb.MigrationState) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	migrationInfo, err := r.store.GetMigrationState(ctx, partitionID)
	if err != nil {
		r.logger.Warn("Failed to get migration state",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return "", pb.MigrationState_MIGRATION_NONE
	}

	if migrationInfo == nil {
		return "", pb.MigrationState_MIGRATION_NONE
	}

	return migrationInfo.Target, migrationInfo.State
}
