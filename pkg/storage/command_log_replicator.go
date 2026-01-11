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
	// CLReplicationInterval is the interval for command log sync
	CLReplicationInterval = 50 * time.Millisecond

	// CLBatchSize is the max entries per replication batch
	CLBatchSize = 500

	// CLReplicationTimeout is the timeout for replication RPC
	CLReplicationTimeout = 5 * time.Second
)

// CommandLogReplicator handles command-log-based replication for a partition
// It uses persistent CommandLog for durability and recovery, with async replication to replica
type CommandLogReplicator struct {
	partitionID uint32
	replicaAddr string
	isPrimary   bool
	logger      *zap.Logger

	// Persistent command log
	commandLog *CommandLog

	// Sequence tracking for replication
	replicatedSeq uint64 // Last successfully replicated sequence

	// gRPC client to replica
	replicaConn   *grpc.ClientConn
	replicaClient pb.StorageServiceClient
	connMu        sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCommandLogReplicator creates a new command-log-based replicator
func NewCommandLogReplicator(partitionID uint32, isPrimary bool, replicaAddr string, logDir string) (*CommandLogReplicator, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create command log for persistence
	commandLog, err := NewCommandLog(partitionID, logDir)
	if err != nil {
		cancel()
		return nil, err
	}

	// Initialize replicatedSeq to current sequence (assume all persisted commands are replicated)
	// In production, this should be read from checkpoint
	replicatedSeq := commandLog.GetCurrentSequence()

	return &CommandLogReplicator{
		partitionID:   partitionID,
		replicaAddr:   replicaAddr,
		isPrimary:     isPrimary,
		logger:        common.NewLogger("cmd-log-replicator"),
		commandLog:    commandLog,
		replicatedSeq: replicatedSeq,
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Start starts the command log replicator
func (clr *CommandLogReplicator) Start() error {
	if !clr.isPrimary || clr.replicaAddr == "" {
		return nil
	}

	// Start replication loop
	clr.wg.Add(1)
	go clr.replicationLoop()

	clr.logger.Info("Command log replicator started",
		zap.Uint32("partition_id", clr.partitionID),
		zap.String("replica_addr", clr.replicaAddr),
		zap.Uint64("current_seq", clr.commandLog.GetCurrentSequence()),
	)

	return nil
}

// Stop stops the replicator
func (clr *CommandLogReplicator) Stop() {
	clr.cancel()
	clr.wg.Wait()

	// Close command log
	if clr.commandLog != nil {
		clr.commandLog.Close()
	}

	clr.connMu.Lock()
	if clr.replicaConn != nil {
		clr.replicaConn.Close()
		clr.replicaConn = nil
	}
	clr.connMu.Unlock()

	clr.logger.Info("Command log replicator stopped",
		zap.Uint32("partition_id", clr.partitionID),
	)
}

// AppendPut appends a put command to the log
func (clr *CommandLogReplicator) AppendPut(key, value []byte) (uint64, error) {
	if clr.commandLog == nil {
		return 0, nil
	}
	return clr.commandLog.AppendPut(key, value)
}

// AppendDelete appends a delete command to the log
func (clr *CommandLogReplicator) AppendDelete(key []byte) (uint64, error) {
	if clr.commandLog == nil {
		return 0, nil
	}
	return clr.commandLog.AppendDelete(key)
}

// AppendFieldBatch appends a field batch update to the log
func (clr *CommandLogReplicator) AppendFieldBatch(batchData []byte) (uint64, error) {
	if clr.commandLog == nil {
		return 0, nil
	}
	return clr.commandLog.AppendFieldBatch(batchData)
}

// GetCurrentSequence returns the current sequence number
func (clr *CommandLogReplicator) GetCurrentSequence() uint64 {
	if clr.commandLog == nil {
		return 0
	}
	return clr.commandLog.GetCurrentSequence()
}

// GetReplicatedSequence returns the last replicated sequence
func (clr *CommandLogReplicator) GetReplicatedSequence() uint64 {
	return atomic.LoadUint64(&clr.replicatedSeq)
}

// GetReplicationLag returns the number of pending entries
func (clr *CommandLogReplicator) GetReplicationLag() uint64 {
	return clr.GetCurrentSequence() - clr.GetReplicatedSequence()
}

// ReplayTo replays all commands from the log to a RocksDB instance
func (clr *CommandLogReplicator) ReplayTo(db *RocksDB, fromSeq uint64) (uint64, error) {
	if clr.commandLog == nil {
		return 0, nil
	}
	return clr.commandLog.ReplayTo(db, fromSeq)
}

// GetCommandLog returns the underlying command log
func (clr *CommandLogReplicator) GetCommandLog() *CommandLog {
	return clr.commandLog
}

// connectToReplica establishes connection to replica
func (clr *CommandLogReplicator) connectToReplica() error {
	clr.connMu.Lock()
	defer clr.connMu.Unlock()

	if clr.replicaConn != nil {
		return nil
	}

	conn, err := grpc.Dial(
		clr.replicaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return err
	}

	clr.replicaConn = conn
	clr.replicaClient = pb.NewStorageServiceClient(conn)
	return nil
}

// replicationLoop runs the main replication loop
func (clr *CommandLogReplicator) replicationLoop() {
	defer clr.wg.Done()

	ticker := time.NewTicker(CLReplicationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-clr.ctx.Done():
			// Final sync before exit
			clr.syncToReplica()
			return

		case <-ticker.C:
			clr.syncToReplica()
		}
	}
}

// syncToReplica sends pending entries to replica
func (clr *CommandLogReplicator) syncToReplica() {
	currentSeq := clr.GetCurrentSequence()
	replicatedSeq := clr.GetReplicatedSequence()

	if currentSeq <= replicatedSeq {
		return // Nothing to replicate
	}

	// Ensure connection
	if err := clr.connectToReplica(); err != nil {
		clr.logger.Warn("Cannot connect to replica",
			zap.Uint32("partition_id", clr.partitionID),
			zap.Error(err),
		)
		return
	}

	// Read entries from command log
	entries, err := clr.commandLog.ReadFrom(replicatedSeq+1, CLBatchSize)
	if err != nil {
		clr.logger.Warn("Failed to read from command log",
			zap.Uint32("partition_id", clr.partitionID),
			zap.Error(err),
		)
		return
	}

	if len(entries) == 0 {
		return
	}

	// Send batch to replica
	if err := clr.sendBatch(entries); err != nil {
		clr.logger.Warn("Failed to replicate batch",
			zap.Uint32("partition_id", clr.partitionID),
			zap.Int("batch_size", len(entries)),
			zap.Error(err),
		)
		// Close connection to force reconnect
		clr.connMu.Lock()
		if clr.replicaConn != nil {
			clr.replicaConn.Close()
			clr.replicaConn = nil
			clr.replicaClient = nil
		}
		clr.connMu.Unlock()
		return
	}

	// Update replicated sequence
	lastSeq := entries[len(entries)-1].Sequence
	atomic.StoreUint64(&clr.replicatedSeq, lastSeq)
}

// sendBatch sends a batch of entries to the replica
func (clr *CommandLogReplicator) sendBatch(entries []*CommandEntry) error {
	clr.connMu.RLock()
	client := clr.replicaClient
	clr.connMu.RUnlock()

	if client == nil {
		return nil
	}

	// Convert to protobuf
	pbEntries := make([]*pb.ReplicationEntry, len(entries))
	var maxOffset int64
	for i, entry := range entries {
		var opType pb.ReplicationOpType
		switch entry.CmdType {
		case CmdTypePut:
			opType = pb.ReplicationOpType_REP_OP_PUT
		case CmdTypeDelete:
			opType = pb.ReplicationOpType_REP_OP_DELETE
		case CmdTypeFieldBatch:
			opType = pb.ReplicationOpType_REP_OP_FIELD_BATCH
		}
		pbEntries[i] = &pb.ReplicationEntry{
			Offset:    int64(entry.Sequence),
			Key:       entry.Key,
			Value:     entry.Value,
			IsDelete:  entry.CmdType == CmdTypeDelete, // Keep for backward compatibility
			Timestamp: entry.Timestamp,
			OpType:    opType,
		}
		if int64(entry.Sequence) > maxOffset {
			maxOffset = int64(entry.Sequence)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), CLReplicationTimeout)
	defer cancel()

	resp, err := client.Replicate(ctx, &pb.ReplicateRequest{
		PartitionId:  clr.partitionID,
		Entries:      pbEntries,
		CommitOffset: maxOffset,
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		clr.logger.Warn("Replica rejected batch",
			zap.Uint32("partition_id", clr.partitionID),
			zap.String("error", resp.ErrorMessage),
		)
	}

	return nil
}

// UpdateReplicaAddr updates the replica address
func (clr *CommandLogReplicator) UpdateReplicaAddr(addr string) {
	clr.connMu.Lock()
	defer clr.connMu.Unlock()

	if clr.replicaAddr == addr {
		return
	}

	clr.replicaAddr = addr

	// Close existing connection
	if clr.replicaConn != nil {
		clr.replicaConn.Close()
		clr.replicaConn = nil
		clr.replicaClient = nil
	}

	clr.logger.Info("Replica address updated",
		zap.Uint32("partition_id", clr.partitionID),
		zap.String("new_addr", addr),
	)
}

// Sync forces a sync of the command log to disk
func (clr *CommandLogReplicator) Sync() error {
	if clr.commandLog == nil {
		return nil
	}
	return clr.commandLog.Sync()
}
