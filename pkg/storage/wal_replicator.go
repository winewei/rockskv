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
	// WALReplicationInterval is the interval for WAL sync
	WALReplicationInterval = 50 * time.Millisecond

	// WALBatchSize is the max entries per batch
	WALBatchSize = 500

	// WALBufferSize is the size of the replication buffer
	WALBufferSize = 100000

	// WALReplicationTimeout is the timeout for replication RPC
	WALReplicationTimeout = 5 * time.Second
)

// WALEntry represents a write operation in the WAL
type WALEntry struct {
	Sequence  uint64
	Key       []byte
	Value     []byte
	IsDelete  bool
	Timestamp int64
}

// WALReplicator handles WAL-based replication for a partition
type WALReplicator struct {
	partitionID uint32
	replicaAddr string
	isPrimary   bool
	logger      *zap.Logger

	// Sequence tracking
	currentSeq    uint64 // Latest write sequence
	replicatedSeq uint64 // Last successfully replicated sequence

	// Write buffer (ring buffer style)
	buffer    []*WALEntry
	bufferMu  sync.RWMutex
	bufferIdx int64 // Next write index

	// gRPC client to replica
	replicaConn   *grpc.ClientConn
	replicaClient pb.StorageServiceClient
	connMu        sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWALReplicator creates a new WAL-based replicator
func NewWALReplicator(partitionID uint32, isPrimary bool, replicaAddr string) *WALReplicator {
	ctx, cancel := context.WithCancel(context.Background())

	return &WALReplicator{
		partitionID: partitionID,
		replicaAddr: replicaAddr,
		isPrimary:   isPrimary,
		logger:      common.NewLogger("wal-replicator"),
		buffer:      make([]*WALEntry, WALBufferSize),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the WAL replicator
func (wr *WALReplicator) Start() error {
	if !wr.isPrimary || wr.replicaAddr == "" {
		return nil
	}

	// Start replication loop
	wr.wg.Add(1)
	go wr.replicationLoop()

	wr.logger.Info("WAL replicator started",
		zap.Uint32("partition_id", wr.partitionID),
		zap.String("replica_addr", wr.replicaAddr),
	)

	return nil
}

// Stop stops the replicator
func (wr *WALReplicator) Stop() {
	wr.cancel()
	wr.wg.Wait()

	wr.connMu.Lock()
	if wr.replicaConn != nil {
		wr.replicaConn.Close()
		wr.replicaConn = nil
	}
	wr.connMu.Unlock()

	wr.logger.Info("WAL replicator stopped",
		zap.Uint32("partition_id", wr.partitionID),
	)
}

// Append adds a write entry to the replication buffer
func (wr *WALReplicator) Append(key, value []byte, isDelete bool) uint64 {
	if !wr.isPrimary {
		return 0
	}

	seq := atomic.AddUint64(&wr.currentSeq, 1)

	entry := &WALEntry{
		Sequence:  seq,
		Key:       key,
		Value:     value,
		IsDelete:  isDelete,
		Timestamp: time.Now().UnixNano(),
	}

	// Write to ring buffer
	idx := atomic.AddInt64(&wr.bufferIdx, 1) - 1
	bufIdx := idx % WALBufferSize

	wr.bufferMu.Lock()
	wr.buffer[bufIdx] = entry
	wr.bufferMu.Unlock()

	return seq
}

// GetCurrentSequence returns the current sequence number
func (wr *WALReplicator) GetCurrentSequence() uint64 {
	return atomic.LoadUint64(&wr.currentSeq)
}

// GetReplicatedSequence returns the last replicated sequence
func (wr *WALReplicator) GetReplicatedSequence() uint64 {
	return atomic.LoadUint64(&wr.replicatedSeq)
}

// GetReplicationLag returns the number of pending entries
func (wr *WALReplicator) GetReplicationLag() uint64 {
	return wr.GetCurrentSequence() - wr.GetReplicatedSequence()
}

// connectToReplica establishes connection to replica
func (wr *WALReplicator) connectToReplica() error {
	wr.connMu.Lock()
	defer wr.connMu.Unlock()

	if wr.replicaConn != nil {
		return nil
	}

	conn, err := grpc.Dial(
		wr.replicaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return err
	}

	wr.replicaConn = conn
	wr.replicaClient = pb.NewStorageServiceClient(conn)
	return nil
}

// replicationLoop runs the main WAL replication loop
func (wr *WALReplicator) replicationLoop() {
	defer wr.wg.Done()

	ticker := time.NewTicker(WALReplicationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wr.ctx.Done():
			// Final sync before exit
			wr.syncToReplica()
			return

		case <-ticker.C:
			wr.syncToReplica()
		}
	}
}

// syncToReplica sends pending entries to replica
func (wr *WALReplicator) syncToReplica() {
	currentSeq := wr.GetCurrentSequence()
	replicatedSeq := wr.GetReplicatedSequence()

	if currentSeq <= replicatedSeq {
		return // Nothing to replicate
	}

	// Ensure connection
	if err := wr.connectToReplica(); err != nil {
		wr.logger.Warn("Cannot connect to replica",
			zap.Uint32("partition_id", wr.partitionID),
			zap.Error(err),
		)
		return
	}

	// Collect entries to replicate
	entries := wr.collectEntries(replicatedSeq+1, currentSeq)
	if len(entries) == 0 {
		return
	}

	// Send in batches
	for i := 0; i < len(entries); i += WALBatchSize {
		end := i + WALBatchSize
		if end > len(entries) {
			end = len(entries)
		}
		batch := entries[i:end]

		if err := wr.sendBatch(batch); err != nil {
			wr.logger.Warn("Failed to replicate batch",
				zap.Uint32("partition_id", wr.partitionID),
				zap.Int("batch_size", len(batch)),
				zap.Error(err),
			)
			// Close connection to force reconnect
			wr.connMu.Lock()
			if wr.replicaConn != nil {
				wr.replicaConn.Close()
				wr.replicaConn = nil
				wr.replicaClient = nil
			}
			wr.connMu.Unlock()
			return
		}

		// Update replicated sequence
		lastSeq := batch[len(batch)-1].Sequence
		atomic.StoreUint64(&wr.replicatedSeq, lastSeq)
	}
}

// collectEntries collects entries from the ring buffer
func (wr *WALReplicator) collectEntries(fromSeq, toSeq uint64) []*WALEntry {
	entries := make([]*WALEntry, 0, toSeq-fromSeq+1)

	wr.bufferMu.RLock()
	defer wr.bufferMu.RUnlock()

	// Scan buffer for matching sequences
	for i := int64(0); i < WALBufferSize; i++ {
		entry := wr.buffer[i]
		if entry == nil {
			continue
		}
		if entry.Sequence >= fromSeq && entry.Sequence <= toSeq {
			entries = append(entries, entry)
		}
	}

	return entries
}

// sendBatch sends a batch of entries to the replica
func (wr *WALReplicator) sendBatch(entries []*WALEntry) error {
	wr.connMu.RLock()
	client := wr.replicaClient
	wr.connMu.RUnlock()

	if client == nil {
		return nil
	}

	// Convert to protobuf
	pbEntries := make([]*pb.ReplicationEntry, len(entries))
	var maxOffset int64
	for i, entry := range entries {
		pbEntries[i] = &pb.ReplicationEntry{
			Offset:    int64(entry.Sequence),
			Key:       entry.Key,
			Value:     entry.Value,
			IsDelete:  entry.IsDelete,
			Timestamp: entry.Timestamp,
		}
		if int64(entry.Sequence) > maxOffset {
			maxOffset = int64(entry.Sequence)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), WALReplicationTimeout)
	defer cancel()

	resp, err := client.Replicate(ctx, &pb.ReplicateRequest{
		PartitionId:  wr.partitionID,
		Entries:      pbEntries,
		CommitOffset: maxOffset,
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		wr.logger.Warn("Replica rejected batch",
			zap.Uint32("partition_id", wr.partitionID),
			zap.String("error", resp.ErrorMessage),
		)
	}

	return nil
}

// UpdateReplicaAddr updates the replica address
func (wr *WALReplicator) UpdateReplicaAddr(addr string) {
	wr.connMu.Lock()
	defer wr.connMu.Unlock()

	if wr.replicaAddr == addr {
		return
	}

	wr.replicaAddr = addr

	// Close existing connection
	if wr.replicaConn != nil {
		wr.replicaConn.Close()
		wr.replicaConn = nil
		wr.replicaClient = nil
	}

	wr.logger.Info("Replica address updated",
		zap.Uint32("partition_id", wr.partitionID),
		zap.String("new_addr", addr),
	)
}
