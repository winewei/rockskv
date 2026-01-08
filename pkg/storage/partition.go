package storage

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"

	"github.com/example/rockskv/pkg/common"
	pb "github.com/example/rockskv/pkg/proto"
)

const (
	// TotalPartitions is the fixed number of partitions
	TotalPartitions = 4096

	// PartitionKeyPrefix is the prefix for partition keys
	PartitionKeyPrefix = "p:"
)

// PartitionManager manages partitions on a storage node
type PartitionManager struct {
	nodeID     string
	partitions map[uint32]*Partition
	mu         sync.RWMutex
	db         *RocksDB
	logger     *zap.Logger
}

// Partition represents a single partition
type Partition struct {
	ID      uint32
	Status  pb.PartitionStatus
	IsPrime bool // true if this node is the primary for this partition
	mu      sync.RWMutex
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(nodeID string, db *RocksDB) *PartitionManager {
	return &PartitionManager{
		nodeID:     nodeID,
		partitions: make(map[uint32]*Partition),
		db:         db,
		logger:     common.NewLogger("partition-manager"),
	}
}

// CalculatePartition calculates the partition ID for a key
func CalculatePartition(key []byte) uint32 {
	return uint32(xxhash.Sum64(key) % TotalPartitions)
}

// MakePartitionKey creates a storage key with partition prefix
func MakePartitionKey(partitionID uint32, key []byte) []byte {
	// Format: "p:<partition_id>:<key>"
	prefix := make([]byte, len(PartitionKeyPrefix)+4)
	copy(prefix, PartitionKeyPrefix)
	binary.BigEndian.PutUint32(prefix[len(PartitionKeyPrefix):], partitionID)

	result := make([]byte, len(prefix)+len(key))
	copy(result, prefix)
	copy(result[len(prefix):], key)

	return result
}

// ParsePartitionKey parses a partition key back to partition ID and original key
func ParsePartitionKey(storageKey []byte) (uint32, []byte, error) {
	prefixLen := len(PartitionKeyPrefix) + 4
	if len(storageKey) < prefixLen {
		return 0, nil, fmt.Errorf("invalid storage key: too short")
	}

	partitionID := binary.BigEndian.Uint32(storageKey[len(PartitionKeyPrefix):prefixLen])
	key := storageKey[prefixLen:]

	return partitionID, key, nil
}

// GetPartitionRange returns the key range for a partition
func GetPartitionRange(partitionID uint32) (start, end []byte) {
	start = make([]byte, len(PartitionKeyPrefix)+4)
	copy(start, PartitionKeyPrefix)
	binary.BigEndian.PutUint32(start[len(PartitionKeyPrefix):], partitionID)

	end = make([]byte, len(PartitionKeyPrefix)+4)
	copy(end, PartitionKeyPrefix)
	binary.BigEndian.PutUint32(end[len(PartitionKeyPrefix):], partitionID+1)

	return start, end
}

// AddPartition adds a partition to this node
func (pm *PartitionManager) AddPartition(partitionID uint32, isPrimary bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[partitionID]; exists {
		return fmt.Errorf("partition %d already exists", partitionID)
	}

	pm.partitions[partitionID] = &Partition{
		ID:      partitionID,
		Status:  pb.PartitionStatus_NORMAL,
		IsPrime: isPrimary,
	}

	pm.logger.Info("Partition added",
		zap.Uint32("partition_id", partitionID),
		zap.Bool("is_primary", isPrimary),
	)

	common.PartitionGauge.WithLabelValues("normal").Inc()
	return nil
}

// RemovePartition removes a partition from this node
func (pm *PartitionManager) RemovePartition(partitionID uint32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	status := partition.Status.String()
	delete(pm.partitions, partitionID)

	pm.logger.Info("Partition removed",
		zap.Uint32("partition_id", partitionID),
	)

	common.PartitionGauge.WithLabelValues(status).Dec()
	return nil
}

// HasPartition checks if this node has the partition
func (pm *PartitionManager) HasPartition(partitionID uint32) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	_, exists := pm.partitions[partitionID]
	return exists
}

// GetPartition returns a partition by ID
func (pm *PartitionManager) GetPartition(partitionID uint32) (*Partition, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	return partition, exists
}

// GetPartitionIDs returns all partition IDs managed by this node
func (pm *PartitionManager) GetPartitionIDs() []uint32 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	ids := make([]uint32, 0, len(pm.partitions))
	for id := range pm.partitions {
		ids = append(ids, id)
	}
	return ids
}

// SetPartitionStatus updates the status of a partition
func (pm *PartitionManager) SetPartitionStatus(partitionID uint32, status pb.PartitionStatus) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	oldStatus := partition.Status
	partition.Status = status

	pm.logger.Info("Partition status changed",
		zap.Uint32("partition_id", partitionID),
		zap.String("old_status", oldStatus.String()),
		zap.String("new_status", status.String()),
	)

	common.PartitionGauge.WithLabelValues(oldStatus.String()).Dec()
	common.PartitionGauge.WithLabelValues(status.String()).Inc()

	return nil
}

// Get retrieves a value from the appropriate partition
func (pm *PartitionManager) Get(key []byte, partitionID uint32) ([]byte, bool, error) {
	if !pm.HasPartition(partitionID) {
		return nil, false, fmt.Errorf("partition %d not found on this node", partitionID)
	}

	storageKey := MakePartitionKey(partitionID, key)
	return pm.db.Get(storageKey)
}

// Put stores a value in the appropriate partition
func (pm *PartitionManager) Put(key, value []byte, partitionID uint32) error {
	if !pm.HasPartition(partitionID) {
		return fmt.Errorf("partition %d not found on this node", partitionID)
	}

	partition, _ := pm.GetPartition(partitionID)
	if partition.Status != pb.PartitionStatus_NORMAL && partition.Status != pb.PartitionStatus_MIGRATING_IN {
		return fmt.Errorf("partition %d is not writable (status: %s)", partitionID, partition.Status.String())
	}

	storageKey := MakePartitionKey(partitionID, key)
	return pm.db.Put(storageKey, value)
}

// Delete removes a value from the appropriate partition
func (pm *PartitionManager) Delete(key []byte, partitionID uint32) error {
	if !pm.HasPartition(partitionID) {
		return fmt.Errorf("partition %d not found on this node", partitionID)
	}

	partition, _ := pm.GetPartition(partitionID)
	if partition.Status != pb.PartitionStatus_NORMAL {
		return fmt.Errorf("partition %d is not writable (status: %s)", partitionID, partition.Status.String())
	}

	storageKey := MakePartitionKey(partitionID, key)
	return pm.db.Delete(storageKey)
}

// BatchPut stores multiple key-value pairs in a partition
func (pm *PartitionManager) BatchPut(items []KeyValueItem, partitionID uint32) error {
	if !pm.HasPartition(partitionID) {
		return fmt.Errorf("partition %d not found on this node", partitionID)
	}

	partition, _ := pm.GetPartition(partitionID)
	if partition.Status != pb.PartitionStatus_NORMAL && partition.Status != pb.PartitionStatus_MIGRATING_IN {
		return fmt.Errorf("partition %d is not writable (status: %s)", partitionID, partition.Status.String())
	}

	// Convert keys to storage keys
	storageItems := make([]KeyValueItem, len(items))
	for i, item := range items {
		storageItems[i] = KeyValueItem{
			Key:   MakePartitionKey(partitionID, item.Key),
			Value: item.Value,
		}
	}

	return pm.db.BatchPut(storageItems)
}

// PartitionCount returns the number of partitions
func (pm *PartitionManager) PartitionCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.partitions)
}

// UpdateFromRouteTable updates partitions based on route table
func (pm *PartitionManager) UpdateFromRouteTable(routeTable *pb.RouteTable) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Track which partitions should exist
	shouldExist := make(map[uint32]bool)

	for _, partition := range routeTable.Partitions {
		isPrimary := partition.Primary == pm.nodeID
		isReplica := partition.Replica == pm.nodeID

		if isPrimary || isReplica {
			shouldExist[partition.PartitionId] = true

			existing, exists := pm.partitions[partition.PartitionId]
			if !exists {
				// Add new partition
				pm.partitions[partition.PartitionId] = &Partition{
					ID:      partition.PartitionId,
					Status:  partition.Status,
					IsPrime: isPrimary,
				}
				common.PartitionGauge.WithLabelValues(partition.Status.String()).Inc()
			} else {
				// Update existing partition
				if existing.Status != partition.Status {
					common.PartitionGauge.WithLabelValues(existing.Status.String()).Dec()
					common.PartitionGauge.WithLabelValues(partition.Status.String()).Inc()
					existing.Status = partition.Status
				}
				existing.IsPrime = isPrimary
			}
		}
	}

	// Remove partitions that should no longer exist
	for id, partition := range pm.partitions {
		if !shouldExist[id] {
			common.PartitionGauge.WithLabelValues(partition.Status.String()).Dec()
			delete(pm.partitions, id)
			pm.logger.Info("Partition removed due to route table update",
				zap.Uint32("partition_id", id),
			)
		}
	}

	return nil
}
