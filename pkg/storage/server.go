package storage

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/linxGnu/grocksdb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/example/rockskv/pkg/common"
	pb "github.com/example/rockskv/pkg/proto"
)

// ServerConfig holds storage server configuration
type ServerConfig struct {
	NodeID       string         `mapstructure:"node_id"`
	ListenAddr   string         `mapstructure:"listen_addr"`
	MetadataAddr string         `mapstructure:"metadata_addr"`
	RocksDB      *RocksDBConfig `mapstructure:"rocksdb"`
	SSTDir       string         `mapstructure:"sst_dir"`
}

// DefaultServerConfig returns a default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		NodeID:       "storage-1",
		ListenAddr:   ":9001",
		MetadataAddr: "localhost:9000",
		RocksDB:      DefaultRocksDBConfig(),
		SSTDir:       "/tmp/rockskv/sst",
	}
}

// Server implements the StorageService gRPC server
type Server struct {
	pb.UnimplementedStorageServiceServer

	config           *ServerConfig
	db               *RocksDB
	partitionManager *PartitionManager
	grpcServer       *grpc.Server
	logger           *zap.Logger
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

	server := &Server{
		config:           config,
		db:               db,
		partitionManager: partitionManager,
		logger:           logger,
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
		grpc.UnaryInterceptor(s.unaryInterceptor),
		grpc.StreamInterceptor(s.streamInterceptor),
	)
	pb.RegisterStorageServiceServer(s.grpcServer, s)

	s.logger.Info("Storage server starting",
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
	if s.db != nil {
		s.db.Close()
	}
	s.logger.Info("Storage server stopped")
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

	common.StorageOperations.WithLabelValues("delete", "success").Inc()
	return &pb.StorageDeleteResponse{
		Success: true,
		Error:   pb.ErrorCode_OK,
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

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if file == nil {
			sstPath = filepath.Join(s.config.SSTDir, chunk.Filename)
			file, err = os.Create(sstPath)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to create sst file: %v", err)
			}
		}

		if len(chunk.Data) > 0 {
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

	if sstPath == "" {
		return status.Errorf(codes.InvalidArgument, "no sst data received")
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
