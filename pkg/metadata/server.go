package metadata

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

// ServerConfig holds metadata server configuration
type ServerConfig struct {
	NodeID     string      `mapstructure:"node_id"`
	ListenAddr string      `mapstructure:"listen_addr"`
	Etcd       *EtcdConfig `mapstructure:"etcd"`
}

// DefaultServerConfig returns a default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		NodeID:     "metadata-1",
		ListenAddr: ":9000",
		Etcd:       DefaultEtcdConfig(),
	}
}

// Server implements the MetadataService gRPC server
type Server struct {
	pb.UnimplementedMetadataServiceServer

	config              *ServerConfig
	store               Store
	router              *Router
	migrationController *MigrationController
	grpcServer          *grpc.Server
	logger              *zap.Logger

	// Subscriber management
	subscribers map[string]pb.MetadataService_SubscribeRouteUpdatesServer
	subMu       sync.RWMutex
}

// NewServer creates a new metadata server
func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	logger := common.NewLogger("metadata-server")

	// Initialize etcd store
	store, err := NewEtcdStore(config.Etcd)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize etcd store: %w", err)
	}

	// Initialize router
	router := NewRouter(store)

	// Initialize migration controller
	migrationController := NewMigrationController(nil, store, router)

	server := &Server{
		config:              config,
		store:               store,
		router:              router,
		migrationController: migrationController,
		logger:              logger,
		subscribers:         make(map[string]pb.MetadataService_SubscribeRouteUpdatesServer),
	}

	return server, nil
}

// Start starts the gRPC server
func (s *Server) Start(ctx context.Context) error {
	// Start router
	if err := s.router.Start(ctx); err != nil {
		return fmt.Errorf("failed to start router: %w", err)
	}

	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,  // Allow pings every 5 seconds
			PermitWithoutStream: true,             // Allow pings even without active streams
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute, // Close idle connections after 15 minutes
			MaxConnectionAge:      30 * time.Minute, // Maximum connection age
			MaxConnectionAgeGrace: 5 * time.Second,  // Grace period for pending RPCs
			Time:                  10 * time.Second, // Ping interval when no activity
			Timeout:               3 * time.Second,  // Ping timeout
		}),
		grpc.UnaryInterceptor(s.unaryInterceptor),
		grpc.StreamInterceptor(s.streamInterceptor),
	)
	pb.RegisterMetadataServiceServer(s.grpcServer, s)

	s.logger.Info("Metadata server starting",
		zap.String("addr", s.config.ListenAddr),
		zap.String("node_id", s.config.NodeID),
	)

	return s.grpcServer.Serve(listener)
}

// Stop gracefully stops the server
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.store != nil {
		_ = s.store.Close()
	}
	s.logger.Info("Metadata server stopped")
}

// unaryInterceptor logs and records metrics for unary RPCs
func (s *Server) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	latency := time.Since(start)
	statusCode := "success"
	if err != nil {
		statusCode = "error"
	}

	common.RequestCounter.WithLabelValues("metadata", info.FullMethod, statusCode).Inc()
	common.RequestLatency.WithLabelValues("metadata", info.FullMethod).Observe(latency.Seconds())

	return resp, err
}

// streamInterceptor logs and records metrics for streaming RPCs
func (s *Server) streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()

	err := handler(srv, ss)

	latency := time.Since(start)
	statusCode := "success"
	if err != nil {
		statusCode = "error"
	}

	common.RequestCounter.WithLabelValues("metadata", info.FullMethod, statusCode).Inc()
	common.RequestLatency.WithLabelValues("metadata", info.FullMethod).Observe(latency.Seconds())

	return err
}

// RegisterNode implements MetadataService.RegisterNode
func (s *Server) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	var role NodeRole
	switch req.Role {
	case pb.NodeRole_STORAGE:
		role = NodeRoleStorage
	case pb.NodeRole_COMPUTE:
		role = NodeRoleCompute
	case pb.NodeRole_METADATA:
		role = NodeRoleMetadata
	}

	node := &NodeInfo{
		ID:   req.NodeId,
		Addr: req.Addr,
		Role: role,
	}

	if err := s.store.RegisterNode(ctx, node); err != nil {
		s.logger.Error("Failed to register node",
			zap.String("node_id", req.NodeId),
			zap.Error(err),
		)
		return &pb.RegisterNodeResponse{Success: false}, nil
	}

	// Update metrics
	common.NodeCount.WithLabelValues(string(role)).Inc()

	s.logger.Info("Node registered",
		zap.String("node_id", req.NodeId),
		zap.String("addr", req.Addr),
		zap.String("role", string(role)),
	)

	// Auto-initialize partitions when enough storage nodes are registered
	if role == NodeRoleStorage {
		s.tryInitializePartitions(ctx)
	}

	return &pb.RegisterNodeResponse{Success: true}, nil
}

// tryInitializePartitions attempts to initialize partitions if not already done
func (s *Server) tryInitializePartitions(ctx context.Context) {
	table := s.router.GetRouteTable()
	if len(table.Partitions) > 0 {
		// Already initialized
		return
	}

	if err := s.router.InitializePartitions(ctx); err != nil {
		s.logger.Debug("Cannot initialize partitions yet",
			zap.Error(err),
		)
		return
	}

	s.logger.Info("Partitions auto-initialized")
}

// Heartbeat implements MetadataService.Heartbeat
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := s.store.UpdateNodeHeartbeat(ctx, req.NodeId); err != nil {
		s.logger.Warn("Heartbeat failed",
			zap.String("node_id", req.NodeId),
			zap.Error(err),
		)
		return &pb.HeartbeatResponse{Success: false}, nil
	}

	table := s.router.GetRouteTable()
	return &pb.HeartbeatResponse{
		Success:      true,
		RouteVersion: table.Version,
	}, nil
}

// GetRouteTable implements MetadataService.GetRouteTable
func (s *Server) GetRouteTable(ctx context.Context, req *pb.GetRouteTableRequest) (*pb.GetRouteTableResponse, error) {
	table := s.router.GetRouteTable()

	// If client has the same version, return empty
	if req.Version >= table.Version {
		return &pb.GetRouteTableResponse{
			RouteTable: &pb.RouteTable{
				Version:    table.Version,
				Partitions: nil,
			},
		}, nil
	}

	// Convert to protobuf format
	pbPartitions := make([]*pb.PartitionInfo, 0, len(table.Partitions))
	for _, partition := range table.Partitions {
		pbPartitions = append(pbPartitions, &pb.PartitionInfo{
			PartitionId:     partition.ID,
			Primary:         partition.Primary,
			Replica:         partition.Replica,
			Status:          convertPartitionStatus(partition.Status),
			MigrationTarget: partition.MigrationTarget,
			MigrationState:  partition.MigrationState,
		})
	}

	return &pb.GetRouteTableResponse{
		RouteTable: &pb.RouteTable{
			Version:    table.Version,
			Partitions: pbPartitions,
		},
	}, nil
}

// SubscribeRouteUpdates implements MetadataService.SubscribeRouteUpdates
func (s *Server) SubscribeRouteUpdates(req *pb.SubscribeRequest, stream pb.MetadataService_SubscribeRouteUpdatesServer) error {
	nodeID := req.NodeId

	// Register subscriber
	s.subMu.Lock()
	s.subscribers[nodeID] = stream
	s.subMu.Unlock()

	s.logger.Info("Node subscribed to route updates", zap.String("node_id", nodeID))

	// Send initial route table
	table := s.router.GetRouteTable()
	if err := s.sendRouteUpdate(stream, table); err != nil {
		s.logger.Error("Failed to send initial route table",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
	}

	// Subscribe to router updates
	updateCh := s.router.Subscribe(nodeID)
	defer s.router.Unsubscribe(nodeID)

	// Stream updates until context is cancelled
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			s.subMu.Lock()
			delete(s.subscribers, nodeID)
			s.subMu.Unlock()
			s.logger.Info("Node unsubscribed from route updates", zap.String("node_id", nodeID))
			return ctx.Err()
		case update, ok := <-updateCh:
			if !ok {
				return nil
			}
			if err := s.sendRouteUpdate(stream, update); err != nil {
				s.logger.Error("Failed to send route update",
					zap.String("node_id", nodeID),
					zap.Error(err),
				)
				return err
			}
		}
	}
}

// sendRouteUpdate sends a route table update to a stream
func (s *Server) sendRouteUpdate(stream pb.MetadataService_SubscribeRouteUpdatesServer, table *RouteTable) error {
	pbPartitions := make([]*pb.PartitionInfo, 0, len(table.Partitions))
	for _, partition := range table.Partitions {
		pbPartitions = append(pbPartitions, &pb.PartitionInfo{
			PartitionId:     partition.ID,
			Primary:         partition.Primary,
			Replica:         partition.Replica,
			Status:          convertPartitionStatus(partition.Status),
			MigrationTarget: partition.MigrationTarget,
			MigrationState:  partition.MigrationState,
		})
	}

	return stream.Send(&pb.RouteUpdate{
		Version:    table.Version,
		Partitions: pbPartitions,
	})
}

// convertPartitionStatus converts internal status to protobuf status
func convertPartitionStatus(status PartitionStatus) pb.PartitionStatus {
	switch status {
	case PartitionStatusNormal:
		return pb.PartitionStatus_NORMAL
	case PartitionStatusMigratingOut:
		return pb.PartitionStatus_MIGRATING_OUT
	case PartitionStatusMigratingIn:
		return pb.PartitionStatus_MIGRATING_IN
	case PartitionStatusOffline:
		return pb.PartitionStatus_OFFLINE
	default:
		return pb.PartitionStatus_NORMAL
	}
}

// GetRouter returns the router
func (s *Server) GetRouter() *Router {
	return s.router
}

// GetStore returns the store
func (s *Server) GetStore() Store {
	return s.store
}

// GetClusterInfo implements MetadataService.GetClusterInfo
func (s *Server) GetClusterInfo(ctx context.Context, req *pb.GetClusterInfoRequest) (*pb.GetClusterInfoResponse, error) {
	info, err := s.store.GetClusterInfo(ctx)
	if err != nil {
		s.logger.Error("Failed to get cluster info", zap.Error(err))
		return nil, err
	}

	nodes, err := s.store.ListNodes(ctx, NodeRoleStorage)
	if err != nil {
		s.logger.Error("Failed to list storage nodes", zap.Error(err))
		return nil, err
	}

	table := s.router.GetRouteTable()

	return &pb.GetClusterInfoResponse{
		State:            convertClusterState(info.State),
		ReplicaCount:     int32(info.ReplicaCount),
		StorageNodeCount: int32(len(nodes)),
		PartitionCount:   int32(len(table.Partitions)),
	}, nil
}

// InitCluster implements MetadataService.InitCluster
func (s *Server) InitCluster(ctx context.Context, req *pb.InitClusterRequest) (*pb.InitClusterResponse, error) {
	if err := s.router.InitCluster(ctx); err != nil {
		s.logger.Error("Failed to initialize cluster", zap.Error(err))
		return &pb.InitClusterResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	nodes, _ := s.store.ListNodes(ctx, NodeRoleStorage)
	table := s.router.GetRouteTable()

	s.logger.Info("Cluster initialized successfully",
		zap.Int("partition_count", len(table.Partitions)),
		zap.Int("node_count", len(nodes)),
	)

	return &pb.InitClusterResponse{
		Success:        true,
		Message:        "cluster initialized successfully",
		PartitionCount: int32(len(table.Partitions)),
		NodeCount:      int32(len(nodes)),
	}, nil
}

// convertClusterState converts internal cluster state to protobuf
func convertClusterState(state ClusterState) pb.ClusterState {
	switch state {
	case ClusterStatePending:
		return pb.ClusterState_CLUSTER_PENDING
	case ClusterStateInitializing:
		return pb.ClusterState_CLUSTER_INITIALIZING
	case ClusterStateRunning:
		return pb.ClusterState_CLUSTER_RUNNING
	default:
		return pb.ClusterState_CLUSTER_PENDING
	}
}

// InitializeCluster initializes the cluster with partition assignments
// Deprecated: Use InitCluster RPC for proper two-phase initialization
func (s *Server) InitializeCluster(ctx context.Context) error {
	return s.router.InitializePartitions(ctx)
}

// TriggerRebalance implements MetadataService.TriggerRebalance
func (s *Server) TriggerRebalance(ctx context.Context, req *pb.TriggerRebalanceRequest) (*pb.TriggerRebalanceResponse, error) {
	return s.migrationController.TriggerRebalance(ctx, req)
}

// GetMigrationStatus implements MetadataService.GetMigrationStatus
func (s *Server) GetMigrationStatus(ctx context.Context, req *pb.GetMigrationStatusRequest) (*pb.GetMigrationStatusResponse, error) {
	return s.migrationController.GetMigrationStatus(ctx, req)
}

// CancelMigration implements MetadataService.CancelMigration
func (s *Server) CancelMigration(ctx context.Context, req *pb.CancelMigrationRequest) (*pb.CancelMigrationResponse, error) {
	return s.migrationController.CancelMigration(ctx, req)
}

// AcquirePartitionLease implements MetadataService.AcquirePartitionLease
func (s *Server) AcquirePartitionLease(ctx context.Context, req *pb.AcquireLeaseRequest) (*pb.AcquireLeaseResponse, error) {
	leaseID, err := s.store.AcquirePartitionLease(ctx, req.PartitionId, req.NodeAddr)
	if err != nil {
		return &pb.AcquireLeaseResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	s.logger.Debug("Partition lease acquired",
		zap.Uint32("partition_id", req.PartitionId),
		zap.String("node_addr", req.NodeAddr),
		zap.Int64("lease_id", leaseID),
	)

	return &pb.AcquireLeaseResponse{
		Success: true,
		LeaseId: leaseID,
	}, nil
}

// RenewPartitionLease implements MetadataService.RenewPartitionLease
func (s *Server) RenewPartitionLease(ctx context.Context, req *pb.RenewLeaseRequest) (*pb.RenewLeaseResponse, error) {
	if err := s.store.RenewPartitionLease(ctx, req.LeaseId); err != nil {
		return &pb.RenewLeaseResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.RenewLeaseResponse{Success: true}, nil
}

// RevokePartitionLease implements MetadataService.RevokePartitionLease
func (s *Server) RevokePartitionLease(ctx context.Context, req *pb.RevokeLeaseRequest) (*pb.RevokeLeaseResponse, error) {
	if err := s.store.RevokePartitionLease(ctx, req.LeaseId); err != nil {
		s.logger.Warn("Failed to revoke partition lease",
			zap.Int64("lease_id", req.LeaseId),
			zap.Error(err),
		)
		return &pb.RevokeLeaseResponse{Success: false}, nil
	}

	return &pb.RevokeLeaseResponse{Success: true}, nil
}

// ShutdownNode implements MetadataService.ShutdownNode
// This performs a controlled shutdown of a storage node
func (s *Server) ShutdownNode(ctx context.Context, req *pb.ShutdownNodeRequest) (*pb.ShutdownNodeResponse, error) {
	s.logger.Info("Shutdown node request received",
		zap.String("node_id", req.NodeId),
		zap.Bool("force", req.Force),
	)

	// Create migration scheduler for this shutdown operation
	var scheduler *MigrationScheduler
	if !req.Force {
		scheduler = NewMigrationScheduler(s.router, s.store)
		scheduler.Start()
		defer scheduler.Stop()
	}

	// Set migration timeout if specified
	var timeoutCtx context.Context
	var cancel context.CancelFunc
	if req.MigrationTimeoutMs > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, time.Duration(req.MigrationTimeoutMs)*time.Millisecond)
	} else {
		// Default 5 minute timeout
		timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Minute)
	}
	defer cancel()

	migrated, failed, err := s.router.ShutdownNode(timeoutCtx, req.NodeId, req.Force, scheduler)
	if err != nil {
		s.logger.Error("Failed to shutdown node",
			zap.String("node_id", req.NodeId),
			zap.Error(err),
		)
		return &pb.ShutdownNodeResponse{
			Success:            false,
			Message:            err.Error(),
			PartitionsMigrated: migrated,
			PartitionsFailed:   failed,
		}, nil
	}

	s.logger.Info("Node shutdown completed",
		zap.String("node_id", req.NodeId),
		zap.Int32("partitions_migrated", migrated),
		zap.Int32("partitions_failed", failed),
	)

	return &pb.ShutdownNodeResponse{
		Success:            true,
		Message:            "node shutdown completed successfully",
		PartitionsMigrated: migrated,
		PartitionsFailed:   failed,
	}, nil
}

// GetNodeStatus implements MetadataService.GetNodeStatus
func (s *Server) GetNodeStatus(ctx context.Context, req *pb.GetNodeStatusRequest) (*pb.GetNodeStatusResponse, error) {
	node, err := s.store.GetNode(ctx, req.NodeId)
	if err != nil {
		s.logger.Error("Failed to get node",
			zap.String("node_id", req.NodeId),
			zap.Error(err),
		)
		return nil, err
	}

	if node == nil {
		return nil, fmt.Errorf("node %s not found", req.NodeId)
	}

	// Get partition counts
	primaryCount, replicaCount := s.router.GetNodePartitionCounts(node.Addr)

	// Convert status
	var status pb.NodeStatus
	switch NodeStatus(node.Status) {
	case NodeStatusOnline:
		status = pb.NodeStatus_NODE_ONLINE
	case NodeStatusOffline:
		status = pb.NodeStatus_NODE_OFFLINE
	case NodeStatusDraining:
		status = pb.NodeStatus_NODE_DRAINING
	case NodeStatusRemoved:
		status = pb.NodeStatus_NODE_REMOVED
	default:
		if node.Status == "online" {
			status = pb.NodeStatus_NODE_ONLINE
		} else {
			status = pb.NodeStatus_NODE_OFFLINE
		}
	}

	isDraining := status == pb.NodeStatus_NODE_DRAINING
	drainingRemaining := int32(0)
	if isDraining {
		drainingRemaining = int32(primaryCount + replicaCount)
	}

	return &pb.GetNodeStatusResponse{
		NodeId:                     node.ID,
		Status:                     status,
		Addr:                       node.Addr,
		LastHeartbeat:              node.LastHeartbeat.UnixNano(),
		PrimaryPartitionCount:      int32(primaryCount),
		ReplicaPartitionCount:      int32(replicaCount),
		IsDraining:                 isDraining,
		DrainingPartitionsRemaining: drainingRemaining,
	}, nil
}
