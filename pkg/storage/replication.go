//go:build cgo && !nocgo
// +build cgo,!nocgo

package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

const (
	// ReplicationBatchSize is the max number of entries to send in one batch
	ReplicationBatchSize = 100

	// ReplicationFlushInterval is the interval to flush pending entries
	ReplicationFlushInterval = 10 * time.Millisecond

	// ReplicationRetryInterval is the interval between retry attempts
	ReplicationRetryInterval = 100 * time.Millisecond

	// MaxPendingEntries is the max number of entries to buffer before blocking
	MaxPendingEntries = 10000
)

// ReplicationEntry represents a write operation to be replicated
type ReplicationEntry struct {
	Offset    int64
	Key       []byte
	Value     []byte
	IsDelete  bool
	Timestamp int64
}

// PartitionReplicator handles replication for a single partition
type PartitionReplicator struct {
	partitionID    uint32
	replicaAddr    string
	isPrimary      bool
	logger         *zap.Logger

	// Offset tracking
	currentOffset   int64 // Latest offset written (Primary) or received (Replica)
	committedOffset int64 // Offset confirmed persisted

	// Pending entries buffer (Primary only)
	pendingEntries chan *ReplicationEntry
	flushSignal    chan struct{}

	// gRPC client to replica (Primary only)
	replicaConn   *grpc.ClientConn
	replicaClient pb.StorageServiceClient

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// NewPartitionReplicator creates a new partition replicator
func NewPartitionReplicator(partitionID uint32, isPrimary bool, replicaAddr string) *PartitionReplicator {
	ctx, cancel := context.WithCancel(context.Background())

	pr := &PartitionReplicator{
		partitionID:     partitionID,
		replicaAddr:     replicaAddr,
		isPrimary:       isPrimary,
		logger:          common.NewLogger("replicator"),
		pendingEntries:  make(chan *ReplicationEntry, MaxPendingEntries),
		flushSignal:     make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
	}

	return pr
}

// Start starts the replicator
func (pr *PartitionReplicator) Start() error {
	if !pr.isPrimary {
		// Replica doesn't need to start replication loop
		return nil
	}

	// Connect to replica
	if pr.replicaAddr != "" {
		if err := pr.connectToReplica(); err != nil {
			pr.logger.Warn("Failed to connect to replica, will retry",
				zap.Uint32("partition_id", pr.partitionID),
				zap.String("replica_addr", pr.replicaAddr),
				zap.Error(err),
			)
		}
	}

	// Start replication loop
	pr.wg.Add(1)
	go pr.replicationLoop()

	pr.logger.Info("Partition replicator started",
		zap.Uint32("partition_id", pr.partitionID),
		zap.Bool("is_primary", pr.isPrimary),
		zap.String("replica_addr", pr.replicaAddr),
	)

	return nil
}

// Stop stops the replicator
func (pr *PartitionReplicator) Stop() {
	pr.cancel()
	pr.wg.Wait()

	pr.mu.Lock()
	if pr.replicaConn != nil {
		pr.replicaConn.Close()
		pr.replicaConn = nil
	}
	pr.mu.Unlock()

	pr.logger.Info("Partition replicator stopped",
		zap.Uint32("partition_id", pr.partitionID),
	)
}

// Enqueue adds an entry to the replication queue (Primary only)
func (pr *PartitionReplicator) Enqueue(entry *ReplicationEntry) {
	if !pr.isPrimary {
		return
	}

	// Assign offset
	entry.Offset = atomic.AddInt64(&pr.currentOffset, 1)
	entry.Timestamp = time.Now().UnixNano()

	select {
	case pr.pendingEntries <- entry:
		// Signal flush if buffer is getting full
		if len(pr.pendingEntries) >= ReplicationBatchSize {
			select {
			case pr.flushSignal <- struct{}{}:
			default:
			}
		}
	default:
		// Buffer full, log warning
		pr.logger.Warn("Replication buffer full, entry dropped",
			zap.Uint32("partition_id", pr.partitionID),
			zap.Int64("offset", entry.Offset),
		)
	}
}

// GetCurrentOffset returns the current offset
func (pr *PartitionReplicator) GetCurrentOffset() int64 {
	return atomic.LoadInt64(&pr.currentOffset)
}

// GetCommittedOffset returns the committed offset
func (pr *PartitionReplicator) GetCommittedOffset() int64 {
	return atomic.LoadInt64(&pr.committedOffset)
}

// GetReplicationLag returns the number of entries pending replication
func (pr *PartitionReplicator) GetReplicationLag() int64 {
	return pr.GetCurrentOffset() - pr.GetCommittedOffset()
}

// SetOffset sets the current offset (used when initializing from snapshot)
func (pr *PartitionReplicator) SetOffset(offset int64) {
	atomic.StoreInt64(&pr.currentOffset, offset)
	atomic.StoreInt64(&pr.committedOffset, offset)
}

// ApplyEntry applies a replicated entry (Replica only)
func (pr *PartitionReplicator) ApplyEntry(entry *ReplicationEntry) error {
	if pr.isPrimary {
		return nil
	}

	// Update offset
	if entry.Offset > atomic.LoadInt64(&pr.currentOffset) {
		atomic.StoreInt64(&pr.currentOffset, entry.Offset)
	}

	return nil
}

// CommitOffset marks entries up to offset as committed
func (pr *PartitionReplicator) CommitOffset(offset int64) {
	if offset > atomic.LoadInt64(&pr.committedOffset) {
		atomic.StoreInt64(&pr.committedOffset, offset)
	}
}

// connectToReplica establishes connection to replica
func (pr *PartitionReplicator) connectToReplica() error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if pr.replicaConn != nil {
		return nil
	}

	conn, err := grpc.Dial(
		pr.replicaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	pr.replicaConn = conn
	pr.replicaClient = pb.NewStorageServiceClient(conn)
	return nil
}

// replicationLoop runs the main replication loop (Primary only)
func (pr *PartitionReplicator) replicationLoop() {
	defer pr.wg.Done()

	ticker := time.NewTicker(ReplicationFlushInterval)
	defer ticker.Stop()

	batch := make([]*ReplicationEntry, 0, ReplicationBatchSize)

	for {
		select {
		case <-pr.ctx.Done():
			// Flush remaining entries before exit
			if len(batch) > 0 {
				pr.sendBatch(batch)
			}
			return

		case entry := <-pr.pendingEntries:
			batch = append(batch, entry)
			if len(batch) >= ReplicationBatchSize {
				pr.sendBatch(batch)
				batch = batch[:0]
			}

		case <-pr.flushSignal:
			if len(batch) > 0 {
				pr.sendBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				pr.sendBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// sendBatch sends a batch of entries to the replica
func (pr *PartitionReplicator) sendBatch(entries []*ReplicationEntry) {
	if len(entries) == 0 {
		return
	}

	pr.mu.RLock()
	client := pr.replicaClient
	pr.mu.RUnlock()

	if client == nil {
		// Try to reconnect
		if err := pr.connectToReplica(); err != nil {
			pr.logger.Warn("Cannot connect to replica",
				zap.Uint32("partition_id", pr.partitionID),
				zap.Error(err),
			)
			return
		}
		pr.mu.RLock()
		client = pr.replicaClient
		pr.mu.RUnlock()
	}

	if client == nil {
		return
	}

	// Convert to protobuf
	pbEntries := make([]*pb.ReplicationEntry, len(entries))
	var maxOffset int64
	for i, entry := range entries {
		pbEntries[i] = &pb.ReplicationEntry{
			Offset:    entry.Offset,
			Key:       entry.Key,
			Value:     entry.Value,
			IsDelete:  entry.IsDelete,
			Timestamp: entry.Timestamp,
		}
		if entry.Offset > maxOffset {
			maxOffset = entry.Offset
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Replicate(ctx, &pb.ReplicateRequest{
		PartitionId:  pr.partitionID,
		Entries:      pbEntries,
		CommitOffset: maxOffset,
	})

	if err != nil {
		pr.logger.Warn("Failed to replicate batch",
			zap.Uint32("partition_id", pr.partitionID),
			zap.Int("batch_size", len(entries)),
			zap.Error(err),
		)
		// Close connection to force reconnect
		pr.mu.Lock()
		if pr.replicaConn != nil {
			pr.replicaConn.Close()
			pr.replicaConn = nil
			pr.replicaClient = nil
		}
		pr.mu.Unlock()
		return
	}

	if resp.Success {
		pr.CommitOffset(resp.LastAppliedOffset)
	} else {
		pr.logger.Warn("Replica rejected batch",
			zap.Uint32("partition_id", pr.partitionID),
			zap.String("error", resp.ErrorMessage),
		)
	}
}

// UpdateReplicaAddr updates the replica address
func (pr *PartitionReplicator) UpdateReplicaAddr(addr string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if pr.replicaAddr == addr {
		return
	}

	pr.replicaAddr = addr

	// Close existing connection
	if pr.replicaConn != nil {
		pr.replicaConn.Close()
		pr.replicaConn = nil
		pr.replicaClient = nil
	}

	pr.logger.Info("Replica address updated",
		zap.Uint32("partition_id", pr.partitionID),
		zap.String("new_addr", addr),
	)
}
