//go:build cgo && !nocgo
// +build cgo,!nocgo

package storage

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

// ServerConfig holds storage server configuration
type ServerConfig struct {
	NodeID       string         `mapstructure:"node_id"`
	ListenAddr   string         `mapstructure:"listen_addr"`
	MetadataAddr string         `mapstructure:"metadata_addr"`
	RocksDB      *RocksDBConfig `mapstructure:"rocksdb"`
	SSTDir       string         `mapstructure:"sst_dir"`
	CommandLogDir string        `mapstructure:"command_log_dir"`
}

// DefaultServerConfig returns a default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		NodeID:        "storage-1",
		ListenAddr:    ":9001",
		MetadataAddr:  "localhost:9000",
		RocksDB:       DefaultRocksDBConfig(),
		SSTDir:        "/tmp/rockskv/sst",
		CommandLogDir: "/tmp/rockskv/cmdlog",
	}
}

// Server implements the StorageService gRPC server
type Server struct {
	pb.UnimplementedStorageServiceServer

	config           *ServerConfig
	db               *RocksDB
	partitionManager *PartitionManager
	grpcServer       *grpc.Server
	metadataConn     *grpc.ClientConn
	metadataClient   pb.MetadataServiceClient
	logger           *zap.Logger

	// Partition leases for split-brain prevention
	// Maps partition ID to lease ID (only for Primary partitions)
	partitionLeases map[uint32]int64
	leaseMu         sync.RWMutex
	leaseCtx        context.Context
	leaseCancel     context.CancelFunc

	// Replication: maps partition ID to command log replicator
	replicators   map[uint32]*CommandLogReplicator
	replicatorsMu sync.RWMutex
}

// NewServer creates a new storage server
func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	logger := common.NewLogger("storage-server")

	// Initialize RocksDB
	db, err := NewRocksDB(config.RocksDB)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize rocksdb: %w", err)
	}

	// Initialize partition manager
	partitionManager := NewPartitionManager(config.NodeID, db)

	// Create SST directory
	if err := os.MkdirAll(config.SSTDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create sst directory: %w", err)
	}

	// Create lease context
	leaseCtx, leaseCancel := context.WithCancel(context.Background())

	// Create command log directory
	if err := os.MkdirAll(config.CommandLogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create command log directory: %w", err)
	}

	server := &Server{
		config:           config,
		db:               db,
		partitionManager: partitionManager,
		logger:           logger,
		partitionLeases:  make(map[uint32]int64),
		leaseCtx:         leaseCtx,
		leaseCancel:      leaseCancel,
		replicators:      make(map[uint32]*CommandLogReplicator),
	}

	return server, nil
}

// Start starts the gRPC server
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.grpcServer = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  10 * time.Second,
			Timeout:               3 * time.Second,
		}),
		grpc.UnaryInterceptor(s.unaryInterceptor),
		grpc.StreamInterceptor(s.streamInterceptor),
	)
	pb.RegisterStorageServiceServer(s.grpcServer, s)

	s.logger.Info("Storage server starting",
		zap.String("addr", s.config.ListenAddr),
		zap.String("node_id", s.config.NodeID),
	)

	// Register with metadata service in background
	go s.registerWithMetadata()

	return s.grpcServer.Serve(listener)
}

// Stop gracefully stops the server
func (s *Server) Stop() {
	// Stop lease renewal loop
	if s.leaseCancel != nil {
		s.leaseCancel()
	}

	// Release all partition leases
	s.releaseAllLeases()

	// Close metadata connection
	if s.metadataConn != nil {
		s.metadataConn.Close()
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.db != nil {
		s.db.Close()
	}
	s.logger.Info("Storage server stopped")
}

// releaseAllLeases releases all held partition leases on shutdown
func (s *Server) releaseAllLeases() {
	if s.metadataClient == nil {
		return
	}

	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	for partitionID, leaseID := range s.partitionLeases {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := s.metadataClient.RevokePartitionLease(ctx, &pb.RevokeLeaseRequest{
			LeaseId: leaseID,
		})
		cancel()

		if err != nil {
			s.logger.Warn("Failed to release partition lease on shutdown",
				zap.Uint32("partition_id", partitionID),
				zap.Int64("lease_id", leaseID),
				zap.Error(err),
			)
		} else {
			s.logger.Debug("Partition lease released",
				zap.Uint32("partition_id", partitionID),
			)
		}
	}

	s.partitionLeases = make(map[uint32]int64)
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

	common.RequestCounter.WithLabelValues("storage", info.FullMethod, statusCode).Inc()
	common.RequestLatency.WithLabelValues("storage", info.FullMethod).Observe(latency.Seconds())

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

	common.RequestCounter.WithLabelValues("storage", info.FullMethod, statusCode).Inc()
	common.RequestLatency.WithLabelValues("storage", info.FullMethod).Observe(latency.Seconds())

	return err
}

// Get implements StorageService.Get
func (s *Server) Get(ctx context.Context, req *pb.StorageGetRequest) (*pb.StorageGetResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("get").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("get", "partition_not_found").Inc()
		return &pb.StorageGetResponse{
			Found: false,
			Error: pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	value, found, err := s.partitionManager.Get(req.Key, req.PartitionId)
	if err != nil {
		s.logger.Error("Get failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
		)
		common.StorageOperations.WithLabelValues("get", "error").Inc()
		return &pb.StorageGetResponse{
			Found: false,
			Error: pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	if !found {
		common.StorageOperations.WithLabelValues("get", "not_found").Inc()
		return &pb.StorageGetResponse{
			Found: false,
			Error: pb.ErrorCode_OK,
		}, nil
	}

	common.StorageOperations.WithLabelValues("get", "success").Inc()
	return &pb.StorageGetResponse{
		Value: value,
		Found: true,
		Error: pb.ErrorCode_OK,
	}, nil
}

// Put implements StorageService.Put
func (s *Server) Put(ctx context.Context, req *pb.StoragePutRequest) (*pb.StoragePutResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("put").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("put", "partition_not_found").Inc()
		return &pb.StoragePutResponse{
			Success: false,
			Error:   pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	// Check if we are the primary for this partition
	if !s.partitionManager.IsPrimary(req.PartitionId) {
		common.StorageOperations.WithLabelValues("put", "not_primary").Inc()
		return &pb.StoragePutResponse{
			Success: false,
			Error:   pb.ErrorCode_NOT_PRIMARY,
		}, nil
	}

	// Validate epoch to prevent split-brain writes
	if err := s.partitionManager.ValidateEpoch(req.PartitionId, req.Epoch); err != nil {
		common.StorageOperations.WithLabelValues("put", "epoch_fenced").Inc()
		return &pb.StoragePutResponse{
			Success: false,
			Error:   pb.ErrorCode_EPOCH_FENCED,
		}, nil
	}

	if err := s.partitionManager.Put(req.Key, req.Value, req.PartitionId); err != nil {
		s.logger.Error("Put failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
		)
		common.StorageOperations.WithLabelValues("put", "error").Inc()
		return &pb.StoragePutResponse{
			Success: false,
			Error:   pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	// Enqueue for async replication (if Primary)
	s.enqueueReplication(req.PartitionId, req.Key, req.Value, false)

	common.StorageOperations.WithLabelValues("put", "success").Inc()
	return &pb.StoragePutResponse{
		Success: true,
		Error:   pb.ErrorCode_OK,
	}, nil
}

// Delete implements StorageService.Delete
func (s *Server) Delete(ctx context.Context, req *pb.StorageDeleteRequest) (*pb.StorageDeleteResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("delete", "partition_not_found").Inc()
		return &pb.StorageDeleteResponse{
			Success: false,
			Error:   pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	// Check if we are the primary for this partition
	if !s.partitionManager.IsPrimary(req.PartitionId) {
		common.StorageOperations.WithLabelValues("delete", "not_primary").Inc()
		return &pb.StorageDeleteResponse{
			Success: false,
			Error:   pb.ErrorCode_NOT_PRIMARY,
		}, nil
	}

	// Validate epoch to prevent split-brain writes
	if err := s.partitionManager.ValidateEpoch(req.PartitionId, req.Epoch); err != nil {
		common.StorageOperations.WithLabelValues("delete", "epoch_fenced").Inc()
		return &pb.StorageDeleteResponse{
			Success: false,
			Error:   pb.ErrorCode_EPOCH_FENCED,
		}, nil
	}

	if err := s.partitionManager.Delete(req.Key, req.PartitionId); err != nil {
		s.logger.Error("Delete failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
		)
		common.StorageOperations.WithLabelValues("delete", "error").Inc()
		return &pb.StorageDeleteResponse{
			Success: false,
			Error:   pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	// Enqueue for async replication (if Primary)
	s.enqueueReplication(req.PartitionId, req.Key, nil, true)

	common.StorageOperations.WithLabelValues("delete", "success").Inc()
	return &pb.StorageDeleteResponse{
		Success: true,
		Error:   pb.ErrorCode_OK,
	}, nil
}

// GetField implements StorageService.GetField - retrieves a single field
func (s *Server) GetField(ctx context.Context, req *pb.StorageGetFieldRequest) (*pb.StorageGetFieldResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("get_field").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("get_field", "partition_not_found").Inc()
		return &pb.StorageGetFieldResponse{
			Found: false,
			Error: pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	fs := NewFieldStorage(s.db, req.PartitionId)
	value, found, err := fs.GetField(req.PrimaryKey, req.FieldName)
	if err != nil {
		s.logger.Error("GetField failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
		)
		common.StorageOperations.WithLabelValues("get_field", "error").Inc()
		return &pb.StorageGetFieldResponse{
			Found: false,
			Error: pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	if !found {
		common.StorageOperations.WithLabelValues("get_field", "not_found").Inc()
		return &pb.StorageGetFieldResponse{
			Found: false,
			Error: pb.ErrorCode_OK,
		}, nil
	}

	common.StorageOperations.WithLabelValues("get_field", "success").Inc()
	return &pb.StorageGetFieldResponse{
		Value: value,
		Found: true,
		Error: pb.ErrorCode_OK,
	}, nil
}

// SetFields implements StorageService.SetFields - sets multiple fields atomically
func (s *Server) SetFields(ctx context.Context, req *pb.StorageSetFieldsRequest) (*pb.StorageSetFieldsResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("set_fields").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("set_fields", "partition_not_found").Inc()
		return &pb.StorageSetFieldsResponse{
			Success: false,
			Error:   pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	// Check if we are the primary for this partition
	if !s.partitionManager.IsPrimary(req.PartitionId) {
		common.StorageOperations.WithLabelValues("set_fields", "not_primary").Inc()
		return &pb.StorageSetFieldsResponse{
			Success: false,
			Error:   pb.ErrorCode_NOT_PRIMARY,
		}, nil
	}

	// Validate epoch to prevent split-brain writes
	if err := s.partitionManager.ValidateEpoch(req.PartitionId, req.Epoch); err != nil {
		common.StorageOperations.WithLabelValues("set_fields", "epoch_fenced").Inc()
		return &pb.StorageSetFieldsResponse{
			Success: false,
			Error:   pb.ErrorCode_EPOCH_FENCED,
		}, nil
	}

	// Build field batch
	fb := &FieldBatch{
		PartitionID: req.PartitionId,
		PrimaryKey:  req.PrimaryKey,
		Updates:     make([]FieldUpdate, 0, len(req.Fields)),
	}
	for _, field := range req.Fields {
		fb.Updates = append(fb.Updates, FieldUpdate{
			FieldName: field.FieldName,
			Value:     field.Value,
			IsDelete:  field.IsDelete,
		})
	}

	// Apply field batch
	fs := NewFieldStorage(s.db, req.PartitionId)
	if err := fs.ApplyFieldBatch(fb); err != nil {
		s.logger.Error("SetFields failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
		)
		common.StorageOperations.WithLabelValues("set_fields", "error").Inc()
		return &pb.StorageSetFieldsResponse{
			Success: false,
			Error:   pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	// Enqueue for async replication
	s.enqueueFieldBatchReplication(req.PartitionId, fb.Encode())

	common.StorageOperations.WithLabelValues("set_fields", "success").Inc()
	return &pb.StorageSetFieldsResponse{
		Success: true,
		Error:   pb.ErrorCode_OK,
	}, nil
}

// DeleteField implements StorageService.DeleteField - deletes a single field
func (s *Server) DeleteField(ctx context.Context, req *pb.StorageDeleteFieldRequest) (*pb.StorageDeleteFieldResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("delete_field").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("delete_field", "partition_not_found").Inc()
		return &pb.StorageDeleteFieldResponse{
			Success: false,
			Error:   pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	// Check if we are the primary for this partition
	if !s.partitionManager.IsPrimary(req.PartitionId) {
		common.StorageOperations.WithLabelValues("delete_field", "not_primary").Inc()
		return &pb.StorageDeleteFieldResponse{
			Success: false,
			Error:   pb.ErrorCode_NOT_PRIMARY,
		}, nil
	}

	// Validate epoch to prevent split-brain writes
	if err := s.partitionManager.ValidateEpoch(req.PartitionId, req.Epoch); err != nil {
		common.StorageOperations.WithLabelValues("delete_field", "epoch_fenced").Inc()
		return &pb.StorageDeleteFieldResponse{
			Success: false,
			Error:   pb.ErrorCode_EPOCH_FENCED,
		}, nil
	}

	fs := NewFieldStorage(s.db, req.PartitionId)
	if err := fs.DeleteField(req.PrimaryKey, req.FieldName); err != nil {
		s.logger.Error("DeleteField failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
		)
		common.StorageOperations.WithLabelValues("delete_field", "error").Inc()
		return &pb.StorageDeleteFieldResponse{
			Success: false,
			Error:   pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	// Enqueue for async replication
	fb := &FieldBatch{
		PartitionID: req.PartitionId,
		PrimaryKey:  req.PrimaryKey,
		Updates: []FieldUpdate{
			{FieldName: req.FieldName, IsDelete: true},
		},
	}
	s.enqueueFieldBatchReplication(req.PartitionId, fb.Encode())

	common.StorageOperations.WithLabelValues("delete_field", "success").Inc()
	return &pb.StorageDeleteFieldResponse{
		Success: true,
		Error:   pb.ErrorCode_OK,
	}, nil
}

// GetAllFields implements StorageService.GetAllFields - retrieves all fields for a document
func (s *Server) GetAllFields(ctx context.Context, req *pb.StorageGetAllFieldsRequest) (*pb.StorageGetAllFieldsResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("get_all_fields").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("get_all_fields", "partition_not_found").Inc()
		return &pb.StorageGetAllFieldsResponse{
			Found: false,
			Error: pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	fs := NewFieldStorage(s.db, req.PartitionId)
	fields, err := fs.GetAllFields(req.PrimaryKey)
	if err != nil {
		s.logger.Error("GetAllFields failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
		)
		common.StorageOperations.WithLabelValues("get_all_fields", "error").Inc()
		return &pb.StorageGetAllFieldsResponse{
			Found: false,
			Error: pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	if len(fields) == 0 {
		common.StorageOperations.WithLabelValues("get_all_fields", "not_found").Inc()
		return &pb.StorageGetAllFieldsResponse{
			Found: false,
			Error: pb.ErrorCode_OK,
		}, nil
	}

	common.StorageOperations.WithLabelValues("get_all_fields", "success").Inc()
	return &pb.StorageGetAllFieldsResponse{
		Fields: fields,
		Found:  true,
		Error:  pb.ErrorCode_OK,
	}, nil
}

// BatchPut implements StorageService.BatchPut
func (s *Server) BatchPut(ctx context.Context, req *pb.StorageBatchPutRequest) (*pb.StorageBatchPutResponse, error) {
	start := time.Now()
	defer func() {
		common.StorageLatency.WithLabelValues("batch_put").Observe(time.Since(start).Seconds())
	}()

	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		common.StorageOperations.WithLabelValues("batch_put", "partition_not_found").Inc()
		return &pb.StorageBatchPutResponse{
			Success: false,
			Error:   pb.ErrorCode_PARTITION_NOT_FOUND,
		}, nil
	}

	// Check if we are the primary for this partition
	if !s.partitionManager.IsPrimary(req.PartitionId) {
		common.StorageOperations.WithLabelValues("batch_put", "not_primary").Inc()
		return &pb.StorageBatchPutResponse{
			Success: false,
			Error:   pb.ErrorCode_NOT_PRIMARY,
		}, nil
	}

	// Validate epoch to prevent split-brain writes
	if err := s.partitionManager.ValidateEpoch(req.PartitionId, req.Epoch); err != nil {
		common.StorageOperations.WithLabelValues("batch_put", "epoch_fenced").Inc()
		return &pb.StorageBatchPutResponse{
			Success: false,
			Error:   pb.ErrorCode_EPOCH_FENCED,
		}, nil
	}

	items := make([]KeyValueItem, len(req.Items))
	for i, item := range req.Items {
		items[i] = KeyValueItem{
			Key:   item.Key,
			Value: item.Value,
		}
	}

	if err := s.partitionManager.BatchPut(items, req.PartitionId); err != nil {
		s.logger.Error("BatchPut failed",
			zap.Error(err),
			zap.Uint32("partition_id", req.PartitionId),
			zap.Int("count", len(items)),
		)
		common.StorageOperations.WithLabelValues("batch_put", "error").Inc()
		return &pb.StorageBatchPutResponse{
			Success: false,
			Error:   pb.ErrorCode_INTERNAL_ERROR,
		}, nil
	}

	// Enqueue each item for replication to Replica
	for _, item := range req.Items {
		s.enqueueReplication(req.PartitionId, item.Key, item.Value, false)
	}

	common.StorageOperations.WithLabelValues("batch_put", "success").Inc()
	return &pb.StorageBatchPutResponse{
		Success: true,
		Count:   int32(len(items)),
		Error:   pb.ErrorCode_OK,
	}, nil
}

// ExportSST implements StorageService.ExportSST
func (s *Server) ExportSST(req *pb.ExportSSTRequest, stream pb.StorageService_ExportSSTServer) error {
	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		return status.Errorf(codes.NotFound, "partition %d not found", req.PartitionId)
	}

	// Mark partition as migrating out
	if err := s.partitionManager.SetPartitionStatus(req.PartitionId, pb.PartitionStatus_MIGRATING_OUT); err != nil {
		return status.Errorf(codes.Internal, "failed to set partition status: %v", err)
	}

	// Create SST file
	sstPath := filepath.Join(s.config.SSTDir, fmt.Sprintf("partition_%d_%d.sst", req.PartitionId, time.Now().UnixNano()))

	// Create SST writer
	envOpts := grocksdb.NewDefaultEnvOptions()
	opts := grocksdb.NewDefaultOptions()
	sstWriter := grocksdb.NewSSTFileWriter(envOpts, opts)
	defer sstWriter.Destroy()

	if err := sstWriter.Open(sstPath); err != nil {
		return status.Errorf(codes.Internal, "failed to open sst writer: %v", err)
	}

	// Create snapshot and iterate partition data
	snapshot := s.db.CreateSnapshot()
	defer s.db.ReleaseSnapshot(snapshot)

	iter := s.db.NewIteratorWithSnapshot(snapshot)
	defer iter.Close()

	startKey, endKey := GetPartitionRange(req.PartitionId)

	keyCount := int64(0)
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		key := iter.Key()
		if compareBytes(key.Data(), endKey) >= 0 {
			key.Free()
			break
		}

		value := iter.Value()
		if err := sstWriter.Put(key.Data(), value.Data()); err != nil {
			key.Free()
			value.Free()
			return status.Errorf(codes.Internal, "failed to write to sst: %v", err)
		}
		key.Free()
		value.Free()
		keyCount++
	}

	if err := iter.Err(); err != nil {
		return status.Errorf(codes.Internal, "iterator error: %v", err)
	}

	// Handle empty partition - send empty chunk to signal successful empty migration
	if keyCount == 0 {
		s.logger.Info("Empty partition, sending empty migration signal",
			zap.Uint32("partition_id", req.PartitionId),
		)
		// Send a single chunk indicating empty partition
		if err := stream.Send(&pb.SSTChunk{
			Filename:    fmt.Sprintf("partition_%d_empty.sst", req.PartitionId),
			IsLast:      true,
			PartitionId: req.PartitionId,
		}); err != nil {
			return err
		}
		os.Remove(sstPath)
		return nil
	}

	if err := sstWriter.Finish(); err != nil {
		return status.Errorf(codes.Internal, "failed to finish sst: %v", err)
	}

	s.logger.Info("SST file created",
		zap.Uint32("partition_id", req.PartitionId),
		zap.String("path", sstPath),
		zap.Int64("keys", keyCount),
	)

	// Stream SST file
	file, err := os.Open(sstPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open sst file: %v", err)
	}
	defer file.Close()
	defer os.Remove(sstPath)

	filename := filepath.Base(sstPath)
	buffer := make([]byte, 64*1024) // 64KB chunks

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read sst file: %v", err)
		}

		chunk := &pb.SSTChunk{
			Data:     buffer[:n],
			Filename: filename,
			IsLast:   false,
		}

		if err := stream.Send(chunk); err != nil {
			return err
		}
	}

	// Send final chunk
	if err := stream.Send(&pb.SSTChunk{
		Filename: filename,
		IsLast:   true,
	}); err != nil {
		return err
	}

	return nil
}

// IngestSST implements StorageService.IngestSST
func (s *Server) IngestSST(stream pb.StorageService_IngestSSTServer) error {
	var sstPath string
	var file *os.File
	var keysIngested int64
	var partitionID uint32
	var hasData bool

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if partitionID == 0 && chunk.PartitionId != 0 {
			partitionID = chunk.PartitionId
		}

		// Handle empty partition migration
		if chunk.IsLast && len(chunk.Data) == 0 && !hasData {
			s.logger.Info("Received empty partition migration",
				zap.Uint32("partition_id", partitionID),
			)
			// Empty partition - just add to partition manager
			if partitionID != 0 && !s.partitionManager.HasPartition(partitionID) {
				if err := s.partitionManager.AddPartition(partitionID, false); err != nil {
					s.logger.Warn("Failed to add empty partition", zap.Error(err))
				}
			}
			return stream.SendAndClose(&pb.IngestSSTResponse{
				Success:      true,
				KeysIngested: 0,
			})
		}

		if file == nil && len(chunk.Data) > 0 {
			sstPath = filepath.Join(s.config.SSTDir, chunk.Filename)
			var err error
			file, err = os.Create(sstPath)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to create sst file: %v", err)
			}
		}

		if len(chunk.Data) > 0 {
			hasData = true
			if _, err := file.Write(chunk.Data); err != nil {
				file.Close()
				os.Remove(sstPath)
				return status.Errorf(codes.Internal, "failed to write sst file: %v", err)
			}
		}

		if chunk.IsLast {
			break
		}
	}

	if file != nil {
		file.Close()
	}

	if sstPath == "" || !hasData {
		// No actual data to ingest
		return stream.SendAndClose(&pb.IngestSSTResponse{
			Success:      true,
			KeysIngested: 0,
		})
	}

	// Ingest SST file
	if err := s.db.IngestExternalFile([]string{sstPath}); err != nil {
		os.Remove(sstPath)
		return status.Errorf(codes.Internal, "failed to ingest sst: %v", err)
	}

	// Clean up
	os.Remove(sstPath)

	s.logger.Info("SST file ingested",
		zap.String("path", sstPath),
		zap.Int64("keys", keysIngested),
	)

	return stream.SendAndClose(&pb.IngestSSTResponse{
		Success:      true,
		KeysIngested: keysIngested,
	})
}

// GetPartitionManager returns the partition manager
func (s *Server) GetPartitionManager() *PartitionManager {
	return s.partitionManager
}

// GetDB returns the RocksDB instance
func (s *Server) GetDB() *RocksDB {
	return s.db
}

// compareBytes compares two byte slices
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// registerWithMetadata connects to metadata service and registers this storage node
func (s *Server) registerWithMetadata() {
	time.Sleep(time.Second)

	for {
		if err := s.doRegister(); err != nil {
			s.logger.Warn("Failed to register with metadata, retrying...",
				zap.Error(err),
			)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	go s.subscribeRouteUpdates()
	go s.leaseRenewalLoop() // Start lease renewal for split-brain prevention
	s.heartbeatLoop()
}

func (s *Server) doRegister() error {
	conn, err := grpc.Dial(
		s.config.MetadataAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to metadata: %w", err)
	}
	s.metadataConn = conn
	s.metadataClient = pb.NewMetadataServiceClient(conn)

	addr := s.config.ListenAddr
	if addr[0] == ':' {
		addr = "localhost" + addr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := s.metadataClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId: s.config.NodeID,
		Addr:   addr,
		Role:   pb.NodeRole_STORAGE,
	})
	if err != nil {
		return fmt.Errorf("register failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("register returned failure")
	}

	s.logger.Info("Registered with metadata service",
		zap.String("metadata_addr", s.config.MetadataAddr),
		zap.String("node_id", s.config.NodeID),
	)

	return nil
}

func (s *Server) heartbeatLoop() {
	// 100ms interval, matching etcd's heartbeat interval
	// 10 missed heartbeats = 1 second timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if s.metadataClient == nil {
			continue
		}

		// Short timeout for heartbeat RPC
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := s.metadataClient.Heartbeat(ctx, &pb.HeartbeatRequest{
			NodeId: s.config.NodeID,
		})
		cancel()

		if err != nil {
			s.logger.Warn("Heartbeat failed", zap.Error(err))
		}
	}
}

func (s *Server) subscribeRouteUpdates() {
	addr := s.config.ListenAddr
	if addr[0] == ':' {
		addr = "localhost" + addr
	}

	for {
		if s.metadataClient == nil {
			time.Sleep(time.Second)
			continue
		}

		stream, err := s.metadataClient.SubscribeRouteUpdates(context.Background(), &pb.SubscribeRequest{
			NodeId: s.config.NodeID,
		})
		if err != nil {
			s.logger.Warn("Failed to subscribe to route updates", zap.Error(err))
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			update, err := stream.Recv()
			if err != nil {
				s.logger.Warn("Route subscription stream error", zap.Error(err))
				break
			}

			s.updatePartitionsFromRoute(update.Partitions, addr)
		}
	}
}

func (s *Server) updatePartitionsFromRoute(partitions []*pb.PartitionInfo, myAddr string) {
	shouldExist := make(map[uint32]bool)
	primaryPartitions := make(map[uint32]bool)
	partitionReplicas := make(map[uint32]string) // partition -> replica address
	addedCount := 0
	removedCount := 0

	for _, p := range partitions {
		isPrimary := p.Primary == myAddr
		isReplica := p.Replica == myAddr

		if isPrimary || isReplica {
			shouldExist[p.PartitionId] = true
			if isPrimary {
				primaryPartitions[p.PartitionId] = true
				partitionReplicas[p.PartitionId] = p.Replica
			}

			if !s.partitionManager.HasPartition(p.PartitionId) {
				if err := s.partitionManager.AddPartitionWithEpoch(p.PartitionId, isPrimary, p.Epoch); err != nil {
					s.logger.Warn("Failed to add partition",
						zap.Uint32("partition_id", p.PartitionId),
						zap.Error(err),
					)
				} else {
					addedCount++
					// Acquire lease for new Primary partition
					if isPrimary {
						s.tryAcquireLease(p.PartitionId, myAddr)
						// Initialize replicator for Primary partition
						s.initializeReplicator(p.PartitionId, true, p.Replica)
					} else {
						// Initialize replicator for Replica partition (receives data)
						s.initializeReplicator(p.PartitionId, false, "")
					}
				}
			} else {
				// Update existing partition's epoch
				s.partitionManager.UpdatePartitionEpoch(p.PartitionId, p.Epoch)
			}
		}
	}

	// Release leases for partitions we're no longer Primary for
	s.leaseMu.Lock()
	for partitionID, leaseID := range s.partitionLeases {
		if !primaryPartitions[partitionID] {
			// We're no longer Primary, release the lease
			s.releaseLeaseAsync(leaseID)
			delete(s.partitionLeases, partitionID)
		}
	}
	s.leaseMu.Unlock()

	currentPartitions := s.partitionManager.ListPartitions()
	for _, partitionID := range currentPartitions {
		if !shouldExist[partitionID] {
			// Stop replicator before removing partition
			s.stopReplicator(partitionID)

			if err := s.partitionManager.RemovePartition(partitionID); err != nil {
				s.logger.Warn("Failed to remove partition",
					zap.Uint32("partition_id", partitionID),
					zap.Error(err),
				)
			} else {
				removedCount++
			}
		}
	}

	if addedCount > 0 || removedCount > 0 {
		s.logger.Info("Partitions updated from route table",
			zap.Int("added", addedCount),
			zap.Int("removed", removedCount),
			zap.Int("total", s.partitionManager.PartitionCount()),
		)
	}
}

// tryAcquireLease attempts to acquire a partition lease
func (s *Server) tryAcquireLease(partitionID uint32, myAddr string) {
	if s.metadataClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := s.metadataClient.AcquirePartitionLease(ctx, &pb.AcquireLeaseRequest{
		PartitionId: partitionID,
		NodeAddr:    myAddr,
	})

	if err != nil || !resp.Success {
		s.logger.Warn("Failed to acquire partition lease",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return
	}

	s.leaseMu.Lock()
	s.partitionLeases[partitionID] = resp.LeaseId
	s.leaseMu.Unlock()

	s.logger.Debug("Partition lease acquired",
		zap.Uint32("partition_id", partitionID),
		zap.Int64("lease_id", resp.LeaseId),
	)
}

// releaseLeaseAsync releases a lease asynchronously
func (s *Server) releaseLeaseAsync(leaseID int64) {
	if s.metadataClient == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		_, _ = s.metadataClient.RevokePartitionLease(ctx, &pb.RevokeLeaseRequest{
			LeaseId: leaseID,
		})
	}()
}

// leaseRenewalLoop periodically renews all held partition leases
func (s *Server) leaseRenewalLoop() {
	// Renew leases every 100ms (TTL is 1s, so 10 chances to renew)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.leaseCtx.Done():
			return
		case <-ticker.C:
			s.renewAllLeases()
		}
	}
}

func (s *Server) renewAllLeases() {
	if s.metadataClient == nil {
		return
	}

	s.leaseMu.RLock()
	leases := make(map[uint32]int64, len(s.partitionLeases))
	for k, v := range s.partitionLeases {
		leases[k] = v
	}
	s.leaseMu.RUnlock()

	for partitionID, leaseID := range leases {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		resp, err := s.metadataClient.RenewPartitionLease(ctx, &pb.RenewLeaseRequest{
			LeaseId: leaseID,
		})
		cancel()

		if err != nil || !resp.Success {
			s.logger.Warn("Failed to renew partition lease",
				zap.Uint32("partition_id", partitionID),
				zap.Int64("lease_id", leaseID),
				zap.Error(err),
			)
			// Remove from map - lease may have expired
			s.leaseMu.Lock()
			delete(s.partitionLeases, partitionID)
			s.leaseMu.Unlock()
		}
	}
}

// enqueueReplication adds an entry to the replication queue for the partition
func (s *Server) enqueueReplication(partitionID uint32, key, value []byte, isDelete bool) {
	s.replicatorsMu.RLock()
	replicator, exists := s.replicators[partitionID]
	s.replicatorsMu.RUnlock()

	if !exists || replicator == nil {
		return
	}

	// Use command-log-based async replication
	var err error
	if isDelete {
		_, err = replicator.AppendDelete(key)
	} else {
		_, err = replicator.AppendPut(key, value)
	}

	if err != nil {
		s.logger.Warn("Failed to append to command log",
			zap.Uint32("partition_id", partitionID),
			zap.Bool("is_delete", isDelete),
			zap.Error(err),
		)
	}
}

// enqueueFieldBatchReplication adds a field batch to the replication queue
func (s *Server) enqueueFieldBatchReplication(partitionID uint32, batchData []byte) {
	s.replicatorsMu.RLock()
	replicator, exists := s.replicators[partitionID]
	s.replicatorsMu.RUnlock()

	if !exists || replicator == nil {
		return
	}

	if _, err := replicator.AppendFieldBatch(batchData); err != nil {
		s.logger.Warn("Failed to append field batch to command log",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
	}
}

// Replicate implements StorageService.Replicate (called on Replica by Primary)
func (s *Server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	// Check if we have the partition
	if !s.partitionManager.HasPartition(req.PartitionId) {
		return &pb.ReplicateResponse{
			Success:      false,
			ErrorMessage: "partition not found",
		}, nil
	}

	// Apply all entries
	var lastApplied int64
	for _, entry := range req.Entries {
		var err error

		// Use op_type if available, fall back to is_delete for backward compatibility
		switch entry.OpType {
		case pb.ReplicationOpType_REP_OP_DELETE:
			err = s.partitionManager.Delete(entry.Key, req.PartitionId)

		case pb.ReplicationOpType_REP_OP_FIELD_BATCH:
			// Apply field batch operation
			fb, decodeErr := DecodeFieldBatch(entry.Value)
			if decodeErr != nil {
				err = decodeErr
				break
			}
			// Use partition ID from FieldBatch (set during encoding) or fallback to request
			partitionID := fb.PartitionID
			if partitionID == 0 {
				partitionID = req.PartitionId
			}
			fs := NewFieldStorage(s.db, partitionID)
			err = fs.ApplyFieldBatch(fb)

		case pb.ReplicationOpType_REP_OP_PUT:
			err = s.partitionManager.Put(entry.Key, entry.Value, req.PartitionId)

		default:
			// Backward compatibility: use is_delete flag
			if entry.IsDelete {
				err = s.partitionManager.Delete(entry.Key, req.PartitionId)
			} else {
				err = s.partitionManager.Put(entry.Key, entry.Value, req.PartitionId)
			}
		}

		if err != nil {
			return &pb.ReplicateResponse{
				Success:           false,
				LastAppliedOffset: lastApplied,
				ErrorMessage:      err.Error(),
			}, nil
		}
		lastApplied = entry.Offset
	}

	return &pb.ReplicateResponse{
		Success:           true,
		LastAppliedOffset: lastApplied,
	}, nil
}

// GetReplicationStatus implements StorageService.GetReplicationStatus
func (s *Server) GetReplicationStatus(ctx context.Context, req *pb.GetReplicationStatusRequest) (*pb.GetReplicationStatusResponse, error) {
	s.replicatorsMu.RLock()
	replicator, exists := s.replicators[req.PartitionId]
	s.replicatorsMu.RUnlock()

	if !exists || replicator == nil {
		return &pb.GetReplicationStatusResponse{
			PartitionId: req.PartitionId,
			IsPrimary:   false,
		}, nil
	}

	replicator.connMu.RLock()
	replicaAddr := replicator.replicaAddr
	replicator.connMu.RUnlock()

	return &pb.GetReplicationStatusResponse{
		PartitionId:     req.PartitionId,
		IsPrimary:       replicator.isPrimary,
		CurrentOffset:   int64(replicator.GetCurrentSequence()),
		CommittedOffset: int64(replicator.GetReplicatedSequence()),
		ReplicationLag:  int64(replicator.GetReplicationLag()),
		ReplicaAddr:     replicaAddr,
	}, nil
}

// RecoverPartition recovers a partition from its command log
func (s *Server) RecoverPartition(partitionID uint32) error {
	s.replicatorsMu.RLock()
	replicator, exists := s.replicators[partitionID]
	s.replicatorsMu.RUnlock()

	if !exists || replicator == nil {
		return fmt.Errorf("no replicator found for partition %d", partitionID)
	}

	// Replay command log to RocksDB
	lastSeq, err := replicator.ReplayTo(s.db, 0)
	if err != nil {
		return fmt.Errorf("failed to replay command log: %w", err)
	}

	s.logger.Info("Partition recovered from command log",
		zap.Uint32("partition_id", partitionID),
		zap.Uint64("last_seq", lastSeq),
	)

	return nil
}

// initializeReplicator creates a command log replicator for a partition
func (s *Server) initializeReplicator(partitionID uint32, isPrimary bool, replicaAddr string) {
	s.replicatorsMu.Lock()
	defer s.replicatorsMu.Unlock()

	// Stop existing replicator if any
	if existing, ok := s.replicators[partitionID]; ok {
		existing.Stop()
	}

	replicator, err := NewCommandLogReplicator(partitionID, isPrimary, replicaAddr, s.config.CommandLogDir)
	if err != nil {
		s.logger.Warn("Failed to create command log replicator",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return
	}

	if err := replicator.Start(); err != nil {
		s.logger.Warn("Failed to start command log replicator",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return
	}

	s.replicators[partitionID] = replicator
}

// stopReplicator stops and removes a replicator for a partition
func (s *Server) stopReplicator(partitionID uint32) {
	s.replicatorsMu.Lock()
	defer s.replicatorsMu.Unlock()

	if replicator, ok := s.replicators[partitionID]; ok {
		replicator.Stop()
		delete(s.replicators, partitionID)
	}
}
