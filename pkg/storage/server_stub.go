//go:build !cgo || nocgo
// +build !cgo nocgo

package storage

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

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
	metadataConn     *grpc.ClientConn
	metadataClient   pb.MetadataServiceClient
	logger           *zap.Logger
}

// NewServer creates a new storage server
func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	logger := common.NewLogger("storage-server")

	// Initialize RocksDB (stub)
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
	pb.RegisterStorageServiceServer(s.grpcServer, s)

	s.logger.Info("Storage server starting (stub mode)",
		zap.String("addr", s.config.ListenAddr),
		zap.String("node_id", s.config.NodeID),
	)

	// Register with metadata service in background
	go s.registerWithMetadata()

	return s.grpcServer.Serve(listener)
}

// registerWithMetadata connects to metadata service and registers this storage node
func (s *Server) registerWithMetadata() {
	// Wait a bit for gRPC server to start
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

	// Subscribe to route table updates
	go s.subscribeRouteUpdates()

	// Start heartbeat loop
	s.heartbeatLoop()
}

func (s *Server) doRegister() error {
	// Connect to metadata service
	conn, err := grpc.Dial(
		s.config.MetadataAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to metadata: %w", err)
	}
	s.metadataConn = conn
	s.metadataClient = pb.NewMetadataServiceClient(conn)

	// Get public address (use listen addr for now)
	addr := s.config.ListenAddr
	if addr[0] == ':' {
		// If only port specified, use localhost
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
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if s.metadataClient == nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := s.metadataClient.Heartbeat(ctx, &pb.HeartbeatRequest{
			NodeId: s.config.NodeID,
		})
		cancel()

		if err != nil {
			s.logger.Warn("Heartbeat failed", zap.Error(err))
		}
	}
}

// subscribeRouteUpdates subscribes to route table updates from metadata service
func (s *Server) subscribeRouteUpdates() {
	// Get public address for matching
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

			// Update partitions based on route table
			s.updatePartitionsFromRoute(update.Partitions, addr)
		}
	}
}

// updatePartitionsFromRoute updates local partitions based on route table
func (s *Server) updatePartitionsFromRoute(partitions []*pb.PartitionInfo, myAddr string) {
	// Track which partitions should exist on this node
	shouldExist := make(map[uint32]bool)
	addedCount := 0
	removedCount := 0

	for _, p := range partitions {
		isPrimary := p.Primary == myAddr
		isReplica := p.Replica == myAddr

		if isPrimary || isReplica {
			shouldExist[p.PartitionId] = true

			if !s.partitionManager.HasPartition(p.PartitionId) {
				if err := s.partitionManager.AddPartition(p.PartitionId, isPrimary); err != nil {
					s.logger.Warn("Failed to add partition",
						zap.Uint32("partition_id", p.PartitionId),
						zap.Error(err),
					)
				} else {
					addedCount++
					s.logger.Debug("Partition added",
						zap.Uint32("partition_id", p.PartitionId),
						zap.Bool("is_primary", isPrimary),
					)
				}
			}
		}
	}

	// Remove partitions that should no longer exist on this node
	currentPartitions := s.partitionManager.ListPartitions()
	for _, partitionID := range currentPartitions {
		if !shouldExist[partitionID] {
			if err := s.partitionManager.RemovePartition(partitionID); err != nil {
				s.logger.Warn("Failed to remove partition",
					zap.Uint32("partition_id", partitionID),
					zap.Error(err),
				)
			} else {
				removedCount++
				s.logger.Info("Partition removed from node",
					zap.Uint32("partition_id", partitionID),
				)
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
	partitionID := req.PartitionId
	bandwidthLimit := req.BandwidthLimit

	s.logger.Info("Starting SST export",
		zap.Uint32("partition_id", partitionID),
		zap.Int64("bandwidth_limit", bandwidthLimit),
	)

	// Get all data for this partition
	data, err := s.partitionManager.ExportPartitionData(partitionID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to export partition data: %v", err)
	}

	totalSize := int64(len(data))
	chunkSize := 4 * 1024 * 1024 // 4MB chunks
	offset := int64(0)

	// Calculate sleep duration for bandwidth limiting
	var sleepDuration time.Duration
	if bandwidthLimit > 0 {
		// Sleep time per chunk to achieve target bandwidth
		sleepDuration = time.Duration(float64(chunkSize) / float64(bandwidthLimit) * float64(time.Second))
	}

	for offset < totalSize {
		end := offset + int64(chunkSize)
		if end > totalSize {
			end = totalSize
		}

		chunk := &pb.SSTChunk{
			Data:        data[offset:end],
			Filename:    fmt.Sprintf("partition_%d.json", partitionID),
			IsLast:      end >= totalSize,
			PartitionId: partitionID,
			TotalSize:   totalSize,
			Offset:      offset,
		}

		if err := stream.Send(chunk); err != nil {
			return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
		}

		offset = end

		// Apply bandwidth limiting
		if sleepDuration > 0 && offset < totalSize {
			time.Sleep(sleepDuration)
		}
	}

	s.logger.Info("SST export completed",
		zap.Uint32("partition_id", partitionID),
		zap.Int64("total_bytes", totalSize),
	)

	return nil
}

// IngestSST implements StorageService.IngestSST
func (s *Server) IngestSST(stream pb.StorageService_IngestSSTServer) error {
	var allData []byte
	var partitionID uint32
	var totalBytes int64

	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}

		if partitionID == 0 {
			partitionID = chunk.PartitionId
		}

		allData = append(allData, chunk.Data...)
		totalBytes += int64(len(chunk.Data))

		if chunk.IsLast {
			break
		}
	}

	if len(allData) == 0 {
		return stream.SendAndClose(&pb.IngestSSTResponse{
			Success:       true,
			KeysIngested:  0,
			BytesIngested: 0,
		})
	}

	// Import the data
	keysIngested, err := s.partitionManager.ImportPartitionData(partitionID, allData)
	if err != nil {
		s.logger.Error("Failed to ingest SST data",
			zap.Uint32("partition_id", partitionID),
			zap.Error(err),
		)
		return stream.SendAndClose(&pb.IngestSSTResponse{
			Success: false,
		})
	}

	s.logger.Info("SST ingest completed",
		zap.Uint32("partition_id", partitionID),
		zap.Int64("keys", keysIngested),
		zap.Int64("bytes", totalBytes),
	)

	return stream.SendAndClose(&pb.IngestSSTResponse{
		Success:       true,
		KeysIngested:  keysIngested,
		BytesIngested: totalBytes,
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
