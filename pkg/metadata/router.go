package metadata

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/example/rockskv/pkg/common"
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

// InitializePartitions initializes partitions across available storage nodes
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
			Primary: nodes[primaryIdx].ID,
			Replica: nodes[replicaIdx].ID,
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

	// Create node ID set for quick lookup
	nodeSet := make(map[string]bool)
	for _, node := range nodes {
		nodeSet[node.ID] = true
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
		if nodeSet[partition.Primary] {
			newPartition.Primary = partition.Primary
		} else {
			// Assign new primary
			newPartition.Primary = nodes[int(partitionID)%nodeCount].ID
			changes++
		}

		// Check if replica is still available and different from primary
		if nodeSet[partition.Replica] && partition.Replica != newPartition.Primary {
			newPartition.Replica = partition.Replica
		} else {
			// Assign new replica
			replicaIdx := (int(partitionID) + 1) % nodeCount
			if nodes[replicaIdx].ID == newPartition.Primary {
				replicaIdx = (replicaIdx + 1) % nodeCount
			}
			newPartition.Replica = nodes[replicaIdx].ID
			changes++
		}

		newTable.Partitions[partitionID] = newPartition
	}

	if changes > 0 {
		if err := r.store.UpdateRouteTable(ctx, newTable); err != nil {
			return fmt.Errorf("failed to save route table: %w", err)
		}

		r.updateRouteTable(newTable)
		r.notifySubscribers(newTable)

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
	partition, ok := r.routeTable.Partitions[partitionID]
	if !ok {
		r.mu.Unlock()
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
	r.mu.Unlock()

	if err := r.store.UpdateRouteTable(ctx, newTable); err != nil {
		return fmt.Errorf("failed to save route table: %w", err)
	}

	r.updateRouteTable(newTable)
	r.notifySubscribers(newTable)

	r.logger.Info("Replica promoted to primary",
		zap.Uint32("partition_id", partitionID),
		zap.String("new_primary", newTable.Partitions[partitionID].Primary),
	)

	return nil
}

// SetPartitionStatus updates the status of a partition
func (r *Router) SetPartitionStatus(ctx context.Context, partitionID uint32, status PartitionStatus) error {
	r.mu.Lock()
	if _, ok := r.routeTable.Partitions[partitionID]; !ok {
		r.mu.Unlock()
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
			ID:      p.ID,
			Primary: p.Primary,
			Replica: p.Replica,
			Status:  p.Status,
		}
	}

	newTable.Partitions[partitionID].Status = status
	r.mu.Unlock()

	if err := r.store.UpdateRouteTable(ctx, newTable); err != nil {
		return fmt.Errorf("failed to save route table: %w", err)
	}

	r.updateRouteTable(newTable)
	r.notifySubscribers(newTable)

	return nil
}
