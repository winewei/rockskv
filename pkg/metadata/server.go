package metadata

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/example/rockskv/pkg/common"
	pb "github.com/example/rockskv/pkg/proto"
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

	config     *ServerConfig
	store      Store
	router     *Router
	grpcServer *grpc.Server
	logger     *zap.Logger

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

	server := &Server{
		config:      config,
		store:       store,
		router:      router,
		logger:      logger,
		subscribers: make(map[string]pb.MetadataService_SubscribeRouteUpdatesServer),
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
			PartitionId: partition.ID,
			Primary:     partition.Primary,
			Replica:     partition.Replica,
			Status:      convertPartitionStatus(partition.Status),
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
			PartitionId: partition.ID,
			Primary:     partition.Primary,
			Replica:     partition.Replica,
			Status:      convertPartitionStatus(partition.Status),
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

// InitializeCluster initializes the cluster with partition assignments
func (s *Server) InitializeCluster(ctx context.Context) error {
	return s.router.InitializePartitions(ctx)
}
