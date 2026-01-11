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

// GetField implements KVService.GetField - retrieves a single field
func (s *Server) GetField(ctx context.Context, req *pb.GetFieldRequest) (*pb.GetFieldResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_get_field").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, replica, partitionID, err := s.router.GetPartitionNodes(req.PrimaryKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Try primary first
	var primaryErr, replicaErr error
	resp, primaryErr := s.doGetField(ctx, primary, req.PrimaryKey, req.FieldName, partitionID)
	if primaryErr == nil {
		return resp, nil
	}

	s.logger.Warn("Primary get_field failed, trying replica",
		zap.String("primary", primary),
		zap.Error(primaryErr),
	)

	// Fallback to replica
	resp, replicaErr = s.doGetField(ctx, replica, req.PrimaryKey, req.FieldName, partitionID)
	if replicaErr == nil {
		return resp, nil
	}

	// Both failed - include both error messages for debugging
	return nil, status.Errorf(codes.Internal, "get_field failed on primary (%v) and replica (%v)", primaryErr, replicaErr)
}

// doGetField performs a get field operation on a specific node
func (s *Server) doGetField(ctx context.Context, nodeID string, pk []byte, fieldName string, partitionID uint32) (*pb.GetFieldResponse, error) {
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

	resp, err := client.GetField(ctx, &pb.StorageGetFieldRequest{
		PrimaryKey:  pk,
		FieldName:   fieldName,
		PartitionId: partitionID,
	})
	if err != nil {
		return nil, fmt.Errorf("storage get_field failed: %w", err)
	}

	if resp.Error != pb.ErrorCode_OK {
		return nil, fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return &pb.GetFieldResponse{
		Value: resp.Value,
		Found: resp.Found,
	}, nil
}

// SetField implements KVService.SetField - sets a single field
func (s *Server) SetField(ctx context.Context, req *pb.SetFieldRequest) (*pb.SetFieldResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_set_field").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, _, partitionID, err := s.router.GetPartitionNodes(req.PrimaryKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Set field only on Primary - Replica will sync via command log replication
	if err := s.doSetFields(ctx, primary, req.PrimaryKey, []*pb.FieldValue{
		{FieldName: req.FieldName, Value: req.Value, IsDelete: false},
	}, partitionID); err != nil {
		s.logger.Error("SetField failed",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "set_field failed: %v", err)
	}

	return &pb.SetFieldResponse{Success: true}, nil
}

// SetFields implements KVService.SetFields - sets multiple fields atomically
func (s *Server) SetFields(ctx context.Context, req *pb.SetFieldsRequest) (*pb.SetFieldsResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_set_fields").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, _, partitionID, err := s.router.GetPartitionNodes(req.PrimaryKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Set fields only on Primary - Replica will sync via command log replication
	if err := s.doSetFields(ctx, primary, req.PrimaryKey, req.Fields, partitionID); err != nil {
		s.logger.Error("SetFields failed",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "set_fields failed: %v", err)
	}

	return &pb.SetFieldsResponse{Success: true}, nil
}

// doSetFields performs a set fields operation on a specific node
func (s *Server) doSetFields(ctx context.Context, nodeID string, pk []byte, fields []*pb.FieldValue, partitionID uint32) error {
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

	resp, err := client.SetFields(ctx, &pb.StorageSetFieldsRequest{
		PrimaryKey:  pk,
		Fields:      fields,
		PartitionId: partitionID,
	})
	if err != nil {
		return fmt.Errorf("storage set_fields failed: %w", err)
	}

	if !resp.Success || resp.Error != pb.ErrorCode_OK {
		return fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return nil
}

// DeleteField implements KVService.DeleteField - deletes a single field
func (s *Server) DeleteField(ctx context.Context, req *pb.DeleteFieldRequest) (*pb.DeleteFieldResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_delete_field").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, _, partitionID, err := s.router.GetPartitionNodes(req.PrimaryKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Delete field only on Primary - Replica will sync via command log replication
	if err := s.doDeleteField(ctx, primary, req.PrimaryKey, req.FieldName, partitionID); err != nil {
		s.logger.Error("DeleteField failed",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "delete_field failed: %v", err)
	}

	return &pb.DeleteFieldResponse{Success: true}, nil
}

// doDeleteField performs a delete field operation on a specific node
func (s *Server) doDeleteField(ctx context.Context, nodeID string, pk []byte, fieldName string, partitionID uint32) error {
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

	resp, err := client.DeleteField(ctx, &pb.StorageDeleteFieldRequest{
		PrimaryKey:  pk,
		FieldName:   fieldName,
		PartitionId: partitionID,
	})
	if err != nil {
		return fmt.Errorf("storage delete_field failed: %w", err)
	}

	if !resp.Success || resp.Error != pb.ErrorCode_OK {
		return fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return nil
}

// GetAllFields implements KVService.GetAllFields - retrieves all fields for a document
func (s *Server) GetAllFields(ctx context.Context, req *pb.GetAllFieldsRequest) (*pb.GetAllFieldsResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("client_get_all_fields").Observe(time.Since(start).Seconds())
	}()

	// Get partition nodes
	primary, replica, partitionID, err := s.router.GetPartitionNodes(req.PrimaryKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
	}

	// Try primary first
	resp, err := s.doGetAllFields(ctx, primary, req.PrimaryKey, partitionID)
	if err == nil {
		return resp, nil
	}

	s.logger.Warn("Primary get_all_fields failed, trying replica",
		zap.String("primary", primary),
		zap.Error(err),
	)

	// Fallback to replica
	resp, err = s.doGetAllFields(ctx, replica, req.PrimaryKey, partitionID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get_all_fields failed on both replicas: %v", err)
	}

	return resp, nil
}

// doGetAllFields performs a get all fields operation on a specific node
func (s *Server) doGetAllFields(ctx context.Context, nodeID string, pk []byte, partitionID uint32) (*pb.GetAllFieldsResponse, error) {
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

	resp, err := client.GetAllFields(ctx, &pb.StorageGetAllFieldsRequest{
		PrimaryKey:  pk,
		PartitionId: partitionID,
	})
	if err != nil {
		return nil, fmt.Errorf("storage get_all_fields failed: %w", err)
	}

	if resp.Error != pb.ErrorCode_OK {
		return nil, fmt.Errorf("storage error: %s", resp.Error.String())
	}

	return &pb.GetAllFieldsResponse{
		Fields: resp.Fields,
		Found:  resp.Found,
	}, nil
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
// Writes only to Primary - Replica syncs via Command Log replication
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
	errCh := make(chan error, len(partitionItems))
	count := int32(0)
	var mu sync.Mutex

	for partitionID, items := range partitionItems {
		primary, _, _, err := s.router.GetPartitionNodesByID(partitionID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "routing failed: %v", err)
		}

		wg.Add(1)

		// Write only to Primary - Replica will sync via Command Log replication
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
		Count:   count,
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
