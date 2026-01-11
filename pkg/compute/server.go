package compute

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
	"github.com/winewei/rockskv/pkg/storage"
)

const (
	// DefaultTimeout is the default timeout for storage operations
	DefaultTimeout = 5 * time.Second
)

// ServerConfig holds compute server configuration
type ServerConfig struct {
	NodeID       string      `mapstructure:"node_id"`
	ListenAddr   string      `mapstructure:"listen_addr"`
	MetadataAddr string      `mapstructure:"metadata_addr"`
	Pool         *PoolConfig `mapstructure:"pool"`
}

// DefaultServerConfig returns a default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		NodeID:       "compute-1",
		ListenAddr:   ":8000",
		MetadataAddr: "localhost:9000",
		Pool:         DefaultPoolConfig(),
	}
}

// Server implements the KVService gRPC server
type Server struct {
	pb.UnimplementedKVServiceServer

	config       *ServerConfig
	router       *Router
	pool         *ConnectionPool
	nodeResolver *NodeResolver
	grpcServer   *grpc.Server
	logger       *zap.Logger
}

// NewServer creates a new compute server
func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	logger := common.NewLogger("compute-server")

	// Initialize router
	router := NewRouter(config.NodeID, config.MetadataAddr)

	// Initialize connection pool
	pool := NewConnectionPool(config.Pool)

	// Initialize node resolver
	nodeResolver := NewNodeResolver(router)

	server := &Server{
		config:       config,
		router:       router,
		pool:         pool,
		nodeResolver: nodeResolver,
		logger:       logger,
	}

	return server, nil
}

// Start starts the gRPC server
func (s *Server) Start() error {
	// Start router
	if err := s.router.Start(); err != nil {
		return fmt.Errorf("failed to start router: %w", err)
	}

	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(s.unaryInterceptor),
	)
	pb.RegisterKVServiceServer(s.grpcServer, s)

	s.logger.Info("Compute server starting",
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
	if s.router != nil {
		s.router.Stop()
	}
	if s.pool != nil {
		s.pool.Close()
	}
	s.logger.Info("Compute server stopped")
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

	common.RequestCounter.WithLabelValues("compute", info.FullMethod, statusCode).Inc()
	common.RequestLatency.WithLabelValues("compute", info.FullMethod).Observe(latency.Seconds())

	return resp, err
}

// Get implements KVService.Get
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_get").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, replica, partitionID, err := s.router.GetPartitionNodes(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Try primary first
	resp, err := s.doGet(ctx, primary, req.Key, partitionID)
	if err == nil {
		return resp, nil
	}

	s.logger.Warn("Primary get failed, trying replica",
		zap.String("primary", primary),
		zap.Error(err),
	)

	// Fallback to replica
	resp, err = s.doGet(ctx, replica, req.Key, partitionID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get failed on both replicas: %v", err)
	}

	return resp, nil
}

// doGet performs a get operation on a specific node
func (s *Server) doGet(ctx context.Context, nodeID string, key []byte, partitionID uint32) (*pb.GetResponse, error) {
	addr, err := s.nodeResolver.ResolveAddr(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve node address: %w", err)
	}

	client, err := s.pool.GetStorageClient(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	resp, err := client.Get(ctx, &pb.StorageGetRequest{
		Key:         key,
		PartitionId: partitionID,
	})
	if err != nil {
		return nil, fmt.Errorf("storage get failed: %w", err)
	}

	if resp.Error != pb.ErrorCode_OK {
		return nil, fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return &pb.GetResponse{
		Value: resp.Value,
		Found: resp.Found,
	}, nil
}

// Put implements KVService.Put - writes only to Primary, Replica syncs via WAL
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_put").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, _, partitionID, err := s.router.GetPartitionNodes(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Write only to Primary - Replica will sync via WAL replication
	if err := s.doPut(ctx, primary, req.Key, req.Value, partitionID); err != nil {
		s.logger.Error("Put failed",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "put failed: %v", err)
	}

	return &pb.PutResponse{Success: true}, nil
}

// doPut performs a put operation on a specific node
func (s *Server) doPut(ctx context.Context, nodeID string, key, value []byte, partitionID uint32) error {
	addr, err := s.nodeResolver.ResolveAddr(nodeID)
	if err != nil {
		return fmt.Errorf("failed to resolve node address: %w", err)
	}

	client, err := s.pool.GetStorageClient(addr)
	if err != nil {
		return fmt.Errorf("failed to get storage client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	resp, err := client.Put(ctx, &pb.StoragePutRequest{
		Key:         key,
		Value:       value,
		PartitionId: partitionID,
	})
	if err != nil {
		return fmt.Errorf("storage put failed: %w", err)
	}

	if !resp.Success || resp.Error != pb.ErrorCode_OK {
		return fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return nil
}

// Delete implements KVService.Delete - deletes only from Primary, Replica syncs via WAL
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_delete").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, _, partitionID, err := s.router.GetPartitionNodes(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Delete only from Primary - Replica will sync via WAL replication
	if err := s.doDelete(ctx, primary, req.Key, partitionID); err != nil {
		s.logger.Error("Delete failed",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}

	return &pb.DeleteResponse{Success: true}, nil
}

// doDelete performs a delete operation on a specific node
func (s *Server) doDelete(ctx context.Context, nodeID string, key []byte, partitionID uint32) error {
	addr, err := s.nodeResolver.ResolveAddr(nodeID)
	if err != nil {
		return fmt.Errorf("failed to resolve node address: %w", err)
	}

	client, err := s.pool.GetStorageClient(addr)
	if err != nil {
		return fmt.Errorf("failed to get storage client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	resp, err := client.Delete(ctx, &pb.StorageDeleteRequest{
		Key:         key,
		PartitionId: partitionID,
	})
	if err != nil {
		return fmt.Errorf("storage delete failed: %w", err)
	}

	if !resp.Success || resp.Error != pb.ErrorCode_OK {
		return fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return nil
}

// Patch implements KVService.Patch - sparse update (partial modification)
func (s *Server) Patch(ctx context.Context, req *pb.PatchRequest) (*pb.PatchResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_patch").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, _, partitionID, err := s.router.GetPartitionNodes(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Convert protobuf operations to internal patch format
	patchData, err := s.convertPatchOperations(req.Operations)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid patch operations: %v", err)
	}

	// Patch only Primary - Replica will sync via WAL replication
	newValue, err := s.doPatch(ctx, primary, req.Key, patchData, partitionID)
	if err != nil {
		s.logger.Error("Patch failed",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "patch failed: %v", err)
	}

	return &pb.PatchResponse{Success: true, NewValue: newValue}, nil
}

// convertPatchOperations converts protobuf patch operations to internal format
func (s *Server) convertPatchOperations(ops []*pb.PatchOperation) ([]byte, error) {
	patch := &storage.Patch{
		Operations: make([]storage.PatchOperation, len(ops)),
	}

	for i, op := range ops {
		var opType storage.PatchOpType
		switch op.Op {
		case pb.PatchOperation_SET:
			opType = storage.PatchOpSet
		case pb.PatchOperation_DELETE:
			opType = storage.PatchOpDelete
		case pb.PatchOperation_INCR:
			opType = storage.PatchOpIncr
		case pb.PatchOperation_APPEND:
			opType = storage.PatchOpAppend
		}
		patch.Operations[i] = storage.PatchOperation{
			Op:    opType,
			Path:  op.Path,
			Value: op.Value,
		}
	}

	return patch.Encode(), nil
}

// doPatch performs a patch operation on a specific node
func (s *Server) doPatch(ctx context.Context, nodeID string, key, patchData []byte, partitionID uint32) ([]byte, error) {
	addr, err := s.nodeResolver.ResolveAddr(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve node address: %w", err)
	}

	client, err := s.pool.GetStorageClient(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	resp, err := client.Patch(ctx, &pb.StoragePatchRequest{
		Key:         key,
		PatchData:   patchData,
		PartitionId: partitionID,
	})
	if err != nil {
		return nil, fmt.Errorf("storage patch failed: %w", err)
	}

	if !resp.Success || resp.Error != pb.ErrorCode_OK {
		return nil, fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return resp.NewValue, nil
}

// BatchGet implements KVService.BatchGet
func (s *Server) BatchGet(ctx context.Context, req *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_batch_get").Observe(time.Since(start).Seconds())
	}()

	items := make([]*pb.KeyValue, len(req.Keys))

	// Process each key
	for i, key := range req.Keys {
		resp, err := s.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			items[i] = &pb.KeyValue{
				Key:   key,
				Found: false,
			}
			continue
		}
		items[i] = &pb.KeyValue{
			Key:   key,
			Value: resp.Value,
			Found: resp.Found,
		}
	}

	return &pb.BatchGetResponse{Items: items}, nil
}

// BatchPut implements KVService.BatchPut
func (s *Server) BatchPut(ctx context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_batch_put").Observe(time.Since(start).Seconds())
	}()

	// Group items by partition
	partitionItems := make(map[uint32][]*pb.KeyValue)
	for _, item := range req.Items {
		partitionID := CalculatePartition(item.Key)
		partitionItems[partitionID] = append(partitionItems[partitionID], item)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(partitionItems)*2)
	count := int32(0)
	var mu sync.Mutex

	for partitionID, items := range partitionItems {
		primary, replica, _, err := s.router.GetPartitionNodesByID(partitionID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
		}

		wg.Add(2)

		// Write to primary
		go func(nodeID string, pid uint32, items []*pb.KeyValue) {
			defer wg.Done()
			if err := s.doBatchPut(ctx, nodeID, items, pid); err != nil {
				errCh <- err
			} else {
				mu.Lock()
				count += int32(len(items))
				mu.Unlock()
			}
		}(primary, partitionID, items)

		// Write to replica
		go func(nodeID string, pid uint32, items []*pb.KeyValue) {
			defer wg.Done()
			if err := s.doBatchPut(ctx, nodeID, items, pid); err != nil {
				errCh <- err
			}
		}(replica, partitionID, items)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		s.logger.Error("BatchPut failed", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "batch put failed: %v", err)
	}

	return &pb.BatchPutResponse{
		Success: true,
		Count:   count / 2, // Divide by 2 since we counted both replicas
	}, nil
}

// doBatchPut performs a batch put operation on a specific node
func (s *Server) doBatchPut(ctx context.Context, nodeID string, items []*pb.KeyValue, partitionID uint32) error {
	addr, err := s.nodeResolver.ResolveAddr(nodeID)
	if err != nil {
		return fmt.Errorf("failed to resolve node address: %w", err)
	}

	client, err := s.pool.GetStorageClient(addr)
	if err != nil {
		return fmt.Errorf("failed to get storage client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	resp, err := client.BatchPut(ctx, &pb.StorageBatchPutRequest{
		Items:       items,
		PartitionId: partitionID,
	})
	if err != nil {
		return fmt.Errorf("storage batch put failed: %w", err)
	}

	if !resp.Success || resp.Error != pb.ErrorCode_OK {
		return fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return nil
}

// GetRouter returns the router
func (s *Server) GetRouter() *Router {
	return s.router
}

// GetPool returns the connection pool
func (s *Server) GetPool() *ConnectionPool {
	return s.pool
}
