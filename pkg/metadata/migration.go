package metadata

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

// MigrationConfig holds migration controller configuration
type MigrationConfig struct {
	MaxConcurrent    int           `mapstructure:"max_concurrent"`     // Max concurrent partition migrations
	BandwidthLimit   int64         `mapstructure:"bandwidth_limit"`    // Bytes per second per migration
	ChunkSize        int           `mapstructure:"chunk_size"`         // SST chunk size in bytes
	CatchupThreshold int64         `mapstructure:"catchup_threshold"`  // Bytes threshold for catchup phase
	RetryAttempts    int           `mapstructure:"retry_attempts"`     // Max retry attempts per partition
	RetryDelay       time.Duration `mapstructure:"retry_delay"`        // Delay between retries
	PauseOnError     bool          `mapstructure:"pause_on_error"`     // Pause remaining migrations if one fails
	Priority         pb.MigrationPriority `mapstructure:"priority"`    // Migration priority
}

// DefaultMigrationConfig returns default migration configuration
func DefaultMigrationConfig() *MigrationConfig {
	return &MigrationConfig{
		MaxConcurrent:    10,
		BandwidthLimit:   200 * 1024 * 1024, // 200 MB/s per migration
		ChunkSize:        4 * 1024 * 1024,   // 4 MB chunks
		CatchupThreshold: 10 * 1024 * 1024,  // 10 MB
		RetryAttempts:    3,
		RetryDelay:       5 * time.Second,
		PauseOnError:     false,
		Priority:         pb.MigrationPriority_PRIORITY_NORMAL,
	}
}

// MigrationState represents the state of a partition migration
type PartitionMigrationState struct {
	PartitionID  uint32
	SourceNode   string
	TargetNode   string
	State        pb.MigrationState
	BytesCopied  int64
	BytesTotal   int64
	StartTime    time.Time
	Error        string
	RetryCount   int
}

// Migration represents an ongoing migration operation
type Migration struct {
	ID                  string
	StartTime           time.Time
	EndTime             time.Time
	TotalPartitions     int32
	CompletedPartitions int32
	FailedPartitions    int32
	BytesCopied         int64
	BytesTotal          int64
	Status              string // "running", "completed", "cancelled", "failed"
	Partitions          map[uint32]*PartitionMigrationState
	mu                  sync.RWMutex
}

// MigrationController manages partition migrations
type MigrationController struct {
	config          *MigrationConfig
	store           Store
	router          *Router
	logger          *zap.Logger
	currentMigration atomic.Pointer[Migration]
	storageConns    map[string]*grpc.ClientConn
	connMu          sync.RWMutex
	cancelFunc      context.CancelFunc
	cancelMu        sync.Mutex
}

// NewMigrationController creates a new migration controller
func NewMigrationController(config *MigrationConfig, store Store, router *Router) *MigrationController {
	if config == nil {
		config = DefaultMigrationConfig()
	}
	return &MigrationController{
		config:       config,
		store:        store,
		router:       router,
		logger:       common.NewLogger("migration-controller"),
		storageConns: make(map[string]*grpc.ClientConn),
	}
}

// TriggerRebalance starts a rebalance operation
func (mc *MigrationController) TriggerRebalance(ctx context.Context, req *pb.TriggerRebalanceRequest) (*pb.TriggerRebalanceResponse, error) {
	// Check if migration is already in progress
	if current := mc.currentMigration.Load(); current != nil && current.Status == "running" {
		return &pb.TriggerRebalanceResponse{
			Success: false,
			Message: "migration already in progress",
		}, nil
	}

	// Get current nodes
	nodes, err := mc.store.ListNodes(ctx, NodeRoleStorage)
	if err != nil {
		return &pb.TriggerRebalanceResponse{
			Success: false,
			Message: fmt.Sprintf("failed to list nodes: %v", err),
		}, nil
	}

	if len(nodes) < MinReplicaNodes {
		return &pb.TriggerRebalanceResponse{
			Success: false,
			Message: fmt.Sprintf("need at least %d storage nodes", MinReplicaNodes),
		}, nil
	}

	// Sort nodes for consistent assignment
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	// Calculate migration plan
	currentTable := mc.router.GetRouteTable()
	plan := mc.calculateMigrationPlan(currentTable, nodes)

	if len(plan) == 0 && !req.Force {
		return &pb.TriggerRebalanceResponse{
			Success: true,
			Message: "cluster is already balanced",
		}, nil
	}

	// Create migration
	migrationID := uuid.New().String()[:8]
	migration := &Migration{
		ID:              migrationID,
		StartTime:       time.Now(),
		TotalPartitions: int32(len(plan)),
		Status:          "running",
		Partitions:      make(map[uint32]*PartitionMigrationState),
	}

	// Calculate total bytes to migrate (estimate)
	for _, p := range plan {
		state := &PartitionMigrationState{
			PartitionID: p.PartitionID,
			SourceNode:  p.SourceNode,
			TargetNode:  p.TargetNode,
			State:       pb.MigrationState_MIGRATION_PREPARING,
			StartTime:   time.Now(),
		}
		migration.Partitions[p.PartitionID] = state
	}

	mc.currentMigration.Store(migration)

	// Apply bandwidth and concurrency limits from request
	config := *mc.config
	if req.BandwidthLimit > 0 {
		config.BandwidthLimit = req.BandwidthLimit
	}
	if req.MaxConcurrent > 0 {
		config.MaxConcurrent = int(req.MaxConcurrent)
	}

	// Start migration in background
	migrationCtx, cancel := context.WithCancel(context.Background())
	mc.cancelMu.Lock()
	mc.cancelFunc = cancel
	mc.cancelMu.Unlock()

	go mc.executeMigration(migrationCtx, migration, plan, &config)

	mc.logger.Info("Migration started",
		zap.String("migration_id", migrationID),
		zap.Int("partitions", len(plan)),
	)

	return &pb.TriggerRebalanceResponse{
		Success:             true,
		Message:             "migration started",
		MigrationId:         migrationID,
		PartitionsToMigrate: int32(len(plan)),
	}, nil
}

// MigrationPlanItem represents a single partition migration
type MigrationPlanItem struct {
	PartitionID uint32
	SourceNode  string
	TargetNode  string
	IsPrimary   bool // true if migrating primary, false if migrating replica
	Priority    pb.MigrationPriority
}

// calculateMigrationPlan calculates which partitions need to be migrated using consistent hashing
func (mc *MigrationController) calculateMigrationPlan(currentTable *RouteTable, nodes []*NodeInfo) []MigrationPlanItem {
	var plan []MigrationPlanItem

	nodeCount := len(nodes)
	if nodeCount == 0 {
		return plan
	}

	// Filter online nodes only
	onlineNodes := make([]*NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		if node.Status == string(NodeStatusOnline) || node.Status == "online" {
			onlineNodes = append(onlineNodes, node)
		}
	}

	if len(onlineNodes) < MinReplicaNodes {
		mc.logger.Warn("Not enough online nodes for rebalance",
			zap.Int("online_nodes", len(onlineNodes)),
			zap.Int("required", MinReplicaNodes),
		)
		return plan
	}

	// Build consistent hash ring with online nodes
	hashRing := NewConsistentHash(DefaultVirtualNodes)
	addrSet := make(map[string]bool)
	for _, node := range onlineNodes {
		hashRing.AddNode(node.Addr)
		addrSet[node.Addr] = true
	}

	// Compare current assignment with consistent hash assignment
	for partitionID, partition := range currentTable.Partitions {
		newPrimary, newReplica := hashRing.GetNodes(partitionID)

		// Check if primary needs to change
		if partition.Primary != newPrimary {
			// Determine priority based on situation
			priority := pb.MigrationPriority_PRIORITY_NORMAL

			// If current primary is offline, high priority
			if !addrSet[partition.Primary] {
				priority = pb.MigrationPriority_PRIORITY_HIGH
			}

			// Prefer promoting existing replica if it matches new primary
			if partition.Replica == newPrimary {
				// Can promote replica instead of migrating
				plan = append(plan, MigrationPlanItem{
					PartitionID: partitionID,
					SourceNode:  partition.Primary,
					TargetNode:  partition.Replica,
					IsPrimary:   true,
					Priority:    priority,
				})
			} else {
				// Need to migrate primary
				plan = append(plan, MigrationPlanItem{
					PartitionID: partitionID,
					SourceNode:  partition.Primary,
					TargetNode:  newPrimary,
					IsPrimary:   true,
					Priority:    priority,
				})
			}
		}

		// Check if replica needs to change
		if partition.Replica != newReplica && partition.Primary != newReplica {
			priority := pb.MigrationPriority_PRIORITY_LOW
			if !addrSet[partition.Replica] {
				priority = pb.MigrationPriority_PRIORITY_NORMAL
			}

			plan = append(plan, MigrationPlanItem{
				PartitionID: partitionID,
				SourceNode:  partition.Replica,
				TargetNode:  newReplica,
				IsPrimary:   false,
				Priority:    priority,
			})
		}
	}

	// Sort plan by priority (higher priority first)
	sort.Slice(plan, func(i, j int) bool {
		return plan[i].Priority > plan[j].Priority
	})

	mc.logger.Info("Migration plan calculated using consistent hashing",
		zap.Int("total_migrations", len(plan)),
		zap.Int("online_nodes", len(onlineNodes)),
	)

	return plan
}

// executeMigration executes the migration plan
func (mc *MigrationController) executeMigration(ctx context.Context, migration *Migration, plan []MigrationPlanItem, config *MigrationConfig) {
	defer func() {
		migration.mu.Lock()
		migration.EndTime = time.Now()
		if migration.Status == "running" {
			if migration.FailedPartitions > 0 {
				migration.Status = "completed_with_errors"
			} else {
				migration.Status = "completed"
			}
		}
		migration.mu.Unlock()

		mc.logger.Info("Migration finished",
			zap.String("migration_id", migration.ID),
			zap.String("status", migration.Status),
			zap.Int32("completed", migration.CompletedPartitions),
			zap.Int32("failed", migration.FailedPartitions),
			zap.Int64("bandwidth_limit", config.BandwidthLimit),
			zap.Int("max_concurrent", config.MaxConcurrent),
		)
	}()

	// Semaphore for concurrent migrations
	sem := make(chan struct{}, config.MaxConcurrent)
	var wg sync.WaitGroup

	// Track if we should pause on error
	var paused atomic.Bool

	mc.logger.Info("Starting migration execution",
		zap.Int("total_partitions", len(plan)),
		zap.Int("max_concurrent", config.MaxConcurrent),
		zap.Int64("bandwidth_limit", config.BandwidthLimit),
		zap.Bool("pause_on_error", config.PauseOnError),
	)

	for _, item := range plan {
		select {
		case <-ctx.Done():
			migration.mu.Lock()
			migration.Status = "cancelled"
			migration.mu.Unlock()
			mc.logger.Info("Migration cancelled by context")
			return
		default:
		}

		// Check if paused due to error
		if config.PauseOnError && paused.Load() {
			migration.mu.Lock()
			migration.Status = "paused_on_error"
			migration.mu.Unlock()
			mc.logger.Warn("Migration paused due to error",
				zap.Int32("completed", migration.CompletedPartitions),
				zap.Int32("failed", migration.FailedPartitions),
			)
			break
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(item MigrationPlanItem) {
			defer func() {
				<-sem
				wg.Done()
			}()

			mc.logger.Debug("Starting partition migration",
				zap.Uint32("partition_id", item.PartitionID),
				zap.String("source", item.SourceNode),
				zap.String("target", item.TargetNode),
				zap.Int32("priority", int32(item.Priority)),
			)

			err := mc.migratePartition(ctx, migration, item, config)

			migration.mu.Lock()
			state := migration.Partitions[item.PartitionID]
			if err != nil {
				state.State = pb.MigrationState_MIGRATION_FAILED
				state.Error = err.Error()
				migration.FailedPartitions++
				mc.logger.Error("Partition migration failed",
					zap.Uint32("partition_id", item.PartitionID),
					zap.Error(err),
				)
				// Set paused flag if pause_on_error is enabled
				if config.PauseOnError {
					paused.Store(true)
				}
			} else {
				state.State = pb.MigrationState_MIGRATION_COMPLETE
				migration.CompletedPartitions++
				mc.logger.Info("Partition migration completed",
					zap.Uint32("partition_id", item.PartitionID),
					zap.Duration("duration", time.Since(state.StartTime)),
				)
			}
			migration.mu.Unlock()
		}(item)
	}

	wg.Wait()
}

// migratePartition migrates a single partition
func (mc *MigrationController) migratePartition(ctx context.Context, migration *Migration, item MigrationPlanItem, config *MigrationConfig) error {
	migration.mu.Lock()
	state := migration.Partitions[item.PartitionID]
	state.State = pb.MigrationState_MIGRATION_PREPARING
	migration.mu.Unlock()

	// Update route table to mark partition as migrating
	if err := mc.router.SetPartitionMigration(ctx, item.PartitionID, item.TargetNode, pb.MigrationState_MIGRATION_COPYING); err != nil {
		return fmt.Errorf("failed to set migration state: %w", err)
	}

	// Get connections to source and target
	sourceClient, err := mc.getStorageClient(item.SourceNode)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

	targetClient, err := mc.getStorageClient(item.TargetNode)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}

	// Phase 1: Export from source
	migration.mu.Lock()
	state.State = pb.MigrationState_MIGRATION_COPYING
	migration.mu.Unlock()

	exportReq := &pb.ExportSSTRequest{
		PartitionId:    item.PartitionID,
		BandwidthLimit: config.BandwidthLimit,
	}

	exportStream, err := sourceClient.ExportSST(ctx, exportReq)
	if err != nil {
		return fmt.Errorf("failed to start export: %w", err)
	}

	// Phase 2: Stream to target
	ingestStream, err := targetClient.IngestSST(ctx)
	if err != nil {
		return fmt.Errorf("failed to start ingest: %w", err)
	}

	var totalBytes int64
	for {
		chunk, err := exportStream.Recv()
		if err != nil {
			break
		}

		if chunk.TotalSize > 0 && state.BytesTotal == 0 {
			migration.mu.Lock()
			state.BytesTotal = chunk.TotalSize
			atomic.AddInt64(&migration.BytesTotal, chunk.TotalSize)
			migration.mu.Unlock()
		}

		if err := ingestStream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}

		totalBytes += int64(len(chunk.Data))
		migration.mu.Lock()
		state.BytesCopied = totalBytes
		atomic.AddInt64(&migration.BytesCopied, int64(len(chunk.Data)))
		migration.mu.Unlock()

		if chunk.IsLast {
			break
		}
	}

	// Close ingest stream
	resp, err := ingestStream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close ingest stream: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("ingest failed")
	}

	// Phase 3: Switch routing
	migration.mu.Lock()
	state.State = pb.MigrationState_MIGRATION_SWITCHING
	migration.mu.Unlock()

	// Update route table to complete migration
	if err := mc.router.CompleteMigration(ctx, item.PartitionID, item.TargetNode, item.IsPrimary); err != nil {
		return fmt.Errorf("failed to complete migration: %w", err)
	}

	return nil
}

// getStorageClient gets or creates a storage client connection
func (mc *MigrationController) getStorageClient(addr string) (pb.StorageServiceClient, error) {
	mc.connMu.RLock()
	conn, exists := mc.storageConns[addr]
	mc.connMu.RUnlock()

	if exists {
		return pb.NewStorageServiceClient(conn), nil
	}

	mc.connMu.Lock()
	defer mc.connMu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := mc.storageConns[addr]; exists {
		return pb.NewStorageServiceClient(conn), nil
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	mc.storageConns[addr] = conn
	return pb.NewStorageServiceClient(conn), nil
}

// GetMigrationStatus returns the current migration status
func (mc *MigrationController) GetMigrationStatus(ctx context.Context, req *pb.GetMigrationStatusRequest) (*pb.GetMigrationStatusResponse, error) {
	migration := mc.currentMigration.Load()
	if migration == nil {
		return &pb.GetMigrationStatusResponse{
			InProgress: false,
		}, nil
	}

	migration.mu.RLock()
	defer migration.mu.RUnlock()

	resp := &pb.GetMigrationStatusResponse{
		InProgress:          migration.Status == "running",
		MigrationId:         migration.ID,
		StartTime:           migration.StartTime.Unix(),
		TotalPartitions:     migration.TotalPartitions,
		CompletedPartitions: migration.CompletedPartitions,
		FailedPartitions:    migration.FailedPartitions,
		BytesCopied:         migration.BytesCopied,
		BytesTotal:          migration.BytesTotal,
	}

	// Calculate progress
	if migration.TotalPartitions > 0 {
		resp.ProgressPercent = float64(migration.CompletedPartitions+migration.FailedPartitions) / float64(migration.TotalPartitions) * 100
	}

	// Estimate remaining time
	if migration.BytesCopied > 0 && migration.Status == "running" {
		elapsed := time.Since(migration.StartTime).Seconds()
		bytesPerSecond := float64(migration.BytesCopied) / elapsed
		if bytesPerSecond > 0 && migration.BytesTotal > migration.BytesCopied {
			remaining := float64(migration.BytesTotal-migration.BytesCopied) / bytesPerSecond
			resp.EstimatedRemainingSeconds = int64(remaining)
		}
	}

	// Add partition status
	for _, state := range migration.Partitions {
		pStatus := &pb.PartitionMigrationStatus{
			PartitionId:  state.PartitionID,
			SourceNode:   state.SourceNode,
			TargetNode:   state.TargetNode,
			State:        state.State,
			BytesCopied:  state.BytesCopied,
			BytesTotal:   state.BytesTotal,
			ErrorMessage: state.Error,
		}
		if state.BytesTotal > 0 {
			pStatus.ProgressPercent = float64(state.BytesCopied) / float64(state.BytesTotal) * 100
		}
		resp.PartitionStatus = append(resp.PartitionStatus, pStatus)
	}

	return resp, nil
}

// CancelMigration cancels an ongoing migration
func (mc *MigrationController) CancelMigration(ctx context.Context, req *pb.CancelMigrationRequest) (*pb.CancelMigrationResponse, error) {
	migration := mc.currentMigration.Load()
	if migration == nil || migration.Status != "running" {
		return &pb.CancelMigrationResponse{
			Success: false,
			Message: "no migration in progress",
		}, nil
	}

	mc.cancelMu.Lock()
	if mc.cancelFunc != nil {
		mc.cancelFunc()
	}
	mc.cancelMu.Unlock()

	migration.mu.Lock()
	migration.Status = "cancelled"
	migration.mu.Unlock()

	return &pb.CancelMigrationResponse{
		Success: true,
		Message: "migration cancelled",
	}, nil
}

// Close closes all connections
func (mc *MigrationController) Close() {
	mc.connMu.Lock()
	defer mc.connMu.Unlock()

	for _, conn := range mc.storageConns {
		conn.Close()
	}
	mc.storageConns = make(map[string]*grpc.ClientConn)
}
