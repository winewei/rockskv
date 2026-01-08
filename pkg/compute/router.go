package compute

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/example/rockskv/pkg/common"
	pb "github.com/example/rockskv/pkg/proto"
)

const (
	// TotalPartitions is the fixed number of partitions
	TotalPartitions = 4096

	// RouteRefreshInterval is the interval for refreshing route table
	RouteRefreshInterval = 30 * time.Second
)

// RouteTable holds routing information
type RouteTable struct {
	Version    uint64
	Partitions map[uint32]*PartitionInfo
}

// PartitionInfo contains routing info for a partition
type PartitionInfo struct {
	ID      uint32
	Primary string
	Replica string
	Status  pb.PartitionStatus
}

// Router handles partition routing
type Router struct {
	metadataAddr string
	metadataConn *grpc.ClientConn
	client       pb.MetadataServiceClient
	nodeID       string

	routeTable atomic.Value // *RouteTable
	mu         sync.RWMutex
	logger     *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRouter creates a new router
func NewRouter(nodeID, metadataAddr string) *Router {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{
		metadataAddr: metadataAddr,
		nodeID:       nodeID,
		logger:       common.NewLogger("compute-router"),
		ctx:          ctx,
		cancel:       cancel,
	}
	r.routeTable.Store(&RouteTable{
		Partitions: make(map[uint32]*PartitionInfo),
	})
	return r
}

// Start starts the router and connects to metadata service
func (r *Router) Start() error {
	// Connect to metadata service
	conn, err := grpc.Dial(
		r.metadataAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to metadata service: %w", err)
	}
	r.metadataConn = conn
	r.client = pb.NewMetadataServiceClient(conn)

	// Initial route table fetch
	if err := r.refreshRouteTable(); err != nil {
		r.logger.Warn("Failed to fetch initial route table", zap.Error(err))
	}

	// Start subscription
	go r.subscribeRouteUpdates()

	// Start periodic refresh as backup
	go r.periodicRefresh()

	r.logger.Info("Router started",
		zap.String("metadata_addr", r.metadataAddr),
	)

	return nil
}

// Stop stops the router
func (r *Router) Stop() {
	r.cancel()
	if r.metadataConn != nil {
		r.metadataConn.Close()
	}
	r.logger.Info("Router stopped")
}

// CalculatePartition calculates the partition ID for a key
func CalculatePartition(key []byte) uint32 {
	return uint32(xxhash.Sum64(key) % TotalPartitions)
}

// GetPartitionNodes returns the primary and replica nodes for a key
func (r *Router) GetPartitionNodes(key []byte) (primary, replica string, partitionID uint32, err error) {
	partitionID = CalculatePartition(key)
	return r.GetPartitionNodesByID(partitionID)
}

// GetPartitionNodesByID returns the primary and replica nodes for a partition ID
func (r *Router) GetPartitionNodesByID(partitionID uint32) (primary, replica string, pid uint32, err error) {
	table := r.routeTable.Load().(*RouteTable)

	partition, ok := table.Partitions[partitionID]
	if !ok {
		return "", "", partitionID, fmt.Errorf("partition %d not found in route table", partitionID)
	}

	return partition.Primary, partition.Replica, partitionID, nil
}

// GetRouteTable returns the current route table
func (r *Router) GetRouteTable() *RouteTable {
	return r.routeTable.Load().(*RouteTable)
}

// refreshRouteTable fetches the latest route table from metadata service
func (r *Router) refreshRouteTable() error {
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	currentTable := r.routeTable.Load().(*RouteTable)
	resp, err := r.client.GetRouteTable(ctx, &pb.GetRouteTableRequest{
		Version: currentTable.Version,
	})
	if err != nil {
		return fmt.Errorf("failed to get route table: %w", err)
	}

	if resp.RouteTable == nil || len(resp.RouteTable.Partitions) == 0 {
		// No update needed
		return nil
	}

	r.updateRouteTable(resp.RouteTable)
	return nil
}

// subscribeRouteUpdates subscribes to route table updates
func (r *Router) subscribeRouteUpdates() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		if err := r.doSubscribe(); err != nil {
			r.logger.Warn("Route subscription error, retrying",
				zap.Error(err),
			)
			time.Sleep(5 * time.Second)
		}
	}
}

// doSubscribe performs the actual subscription
func (r *Router) doSubscribe() error {
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	stream, err := r.client.SubscribeRouteUpdates(ctx, &pb.SubscribeRequest{
		NodeId: r.nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	for {
		update, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream error: %w", err)
		}

		r.updateRouteTableFromUpdate(update)
	}
}

// updateRouteTable updates the route table from a response
func (r *Router) updateRouteTable(pbTable *pb.RouteTable) {
	newTable := &RouteTable{
		Version:    pbTable.Version,
		Partitions: make(map[uint32]*PartitionInfo),
	}

	for _, p := range pbTable.Partitions {
		newTable.Partitions[p.PartitionId] = &PartitionInfo{
			ID:      p.PartitionId,
			Primary: p.Primary,
			Replica: p.Replica,
			Status:  p.Status,
		}
	}

	r.routeTable.Store(newTable)
	common.RouteTableVersion.Set(float64(newTable.Version))

	r.logger.Info("Route table updated",
		zap.Uint64("version", newTable.Version),
		zap.Int("partitions", len(newTable.Partitions)),
	)
}

// updateRouteTableFromUpdate updates the route table from an update
func (r *Router) updateRouteTableFromUpdate(update *pb.RouteUpdate) {
	currentTable := r.routeTable.Load().(*RouteTable)

	// Skip if we already have this version
	if update.Version <= currentTable.Version {
		return
	}

	newTable := &RouteTable{
		Version:    update.Version,
		Partitions: make(map[uint32]*PartitionInfo),
	}

	// Copy existing partitions
	for id, p := range currentTable.Partitions {
		newTable.Partitions[id] = p
	}

	// Apply updates
	for _, p := range update.Partitions {
		newTable.Partitions[p.PartitionId] = &PartitionInfo{
			ID:      p.PartitionId,
			Primary: p.Primary,
			Replica: p.Replica,
			Status:  p.Status,
		}
	}

	r.routeTable.Store(newTable)
	common.RouteTableVersion.Set(float64(newTable.Version))

	r.logger.Info("Route table updated from subscription",
		zap.Uint64("version", newTable.Version),
	)
}

// periodicRefresh periodically refreshes the route table as a backup
func (r *Router) periodicRefresh() {
	ticker := time.NewTicker(RouteRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if err := r.refreshRouteTable(); err != nil {
				r.logger.Warn("Periodic route table refresh failed", zap.Error(err))
			}
		}
	}
}
