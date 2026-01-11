package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
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
	IsPrime bool   // true if this node is the primary for this partition
	Epoch   uint64 // Fencing token for split-brain prevention
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
// Returns (partitionID, originalKey, isFieldKey, error)
func ParsePartitionKey(storageKey []byte) (uint32, []byte, error) {
	prefixLen := len(PartitionKeyPrefix) + 4
	if len(storageKey) < prefixLen {
		return 0, nil, fmt.Errorf("invalid storage key: too short")
	}

	partitionID := binary.BigEndian.Uint32(storageKey[len(PartitionKeyPrefix):prefixLen])
	key := storageKey[prefixLen:]

	return partitionID, key, nil
}

// FieldKeyMarkerByte is the marker byte for field keys ('f')
// Note: This is duplicated from field_storage.go to avoid circular dependency
const FieldKeyMarkerByte byte = 'f'

// ParsePartitionKeyEx parses a partition key with field key detection
// Returns (partitionID, originalKey, isFieldKey, error)
func ParsePartitionKeyEx(storageKey []byte) (uint32, []byte, bool, error) {
	prefixLen := len(PartitionKeyPrefix) + 4 // "p:" + 4 bytes
	if len(storageKey) < prefixLen {
		return 0, nil, false, fmt.Errorf("invalid storage key: too short")
	}

	partitionID := binary.BigEndian.Uint32(storageKey[len(PartitionKeyPrefix):prefixLen])

	// Check if this is a field key (has ":f" marker after partition ID)
	// Field key format: p:<partition_id>:f<pk_len><pk><field_name>
	if len(storageKey) > prefixLen+1 &&
		storageKey[prefixLen] == ':' &&
		storageKey[prefixLen+1] == FieldKeyMarkerByte {
		// This is a field key - return the raw suffix for field key handling
		return partitionID, storageKey[prefixLen:], true, nil
	}

	// Regular KV key
	return partitionID, storageKey[prefixLen:], false, nil
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

// AddPartitionWithEpoch adds a partition to this node with an initial epoch
func (pm *PartitionManager) AddPartitionWithEpoch(partitionID uint32, isPrimary bool, epoch uint64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[partitionID]; exists {
		return fmt.Errorf("partition %d already exists", partitionID)
	}

	pm.partitions[partitionID] = &Partition{
		ID:      partitionID,
		Status:  pb.PartitionStatus_NORMAL,
		IsPrime: isPrimary,
		Epoch:   epoch,
	}

	pm.logger.Info("Partition added with epoch",
		zap.Uint32("partition_id", partitionID),
		zap.Bool("is_primary", isPrimary),
		zap.Uint64("epoch", epoch),
	)

	common.PartitionGauge.WithLabelValues("normal").Inc()
	return nil
}

// UpdatePartitionEpoch updates the epoch of a partition
func (pm *PartitionManager) UpdatePartitionEpoch(partitionID uint32, epoch uint64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	if epoch > partition.Epoch {
		pm.logger.Info("Partition epoch updated",
			zap.Uint32("partition_id", partitionID),
			zap.Uint64("old_epoch", partition.Epoch),
			zap.Uint64("new_epoch", epoch),
		)
		partition.Epoch = epoch
	}

	return nil
}

// ValidateEpoch checks if the request epoch matches the partition epoch (for write requests)
func (pm *PartitionManager) ValidateEpoch(partitionID uint32, requestEpoch uint64) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	if requestEpoch != partition.Epoch {
		pm.logger.Warn("Epoch mismatch - request fenced",
			zap.Uint32("partition_id", partitionID),
			zap.Uint64("request_epoch", requestEpoch),
			zap.Uint64("current_epoch", partition.Epoch),
		)
		return fmt.Errorf("epoch mismatch: request=%d, current=%d", requestEpoch, partition.Epoch)
	}

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

// IsPrimary checks if this node is the primary for the given partition
func (pm *PartitionManager) IsPrimary(partitionID uint32) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return false
	}
	return partition.IsPrime
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

// ListPartitions returns a list of all partition IDs on this node
func (pm *PartitionManager) ListPartitions() []uint32 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partitions := make([]uint32, 0, len(pm.partitions))
	for id := range pm.partitions {
		partitions = append(partitions, id)
	}
	return partitions
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

// PartitionData represents serializable partition data
type PartitionData struct {
	PartitionID uint32            `json:"partition_id"`
	KeyValues   map[string]string `json:"key_values"`
}

// ExportPartitionData exports all data from a partition as JSON bytes
func (pm *PartitionManager) ExportPartitionData(partitionID uint32) ([]byte, error) {
	if !pm.HasPartition(partitionID) {
		return nil, fmt.Errorf("partition %d not found on this node", partitionID)
	}

	// Get the key range for this partition
	startKey, endKey := GetPartitionRange(partitionID)

	// Iterate through all keys in the range
	data := &PartitionData{
		PartitionID: partitionID,
		KeyValues:   make(map[string]string),
	}

	iter := pm.db.NewIterator()
	defer iter.Close()

	iter.Seek(startKey)
	for iter.Valid() {
		key := iter.Key().Data()

		// Check if we've passed the end of the partition range
		if len(key) >= len(endKey) {
			keyPrefix := key[:len(endKey)]
			if string(keyPrefix) >= string(endKey) {
				break
			}
		}

		// Parse the key to extract the original key
		pID, originalKey, err := ParsePartitionKey(key)
		if err != nil || pID != partitionID {
			iter.Next()
			continue
		}

		value := iter.Value().Data()
		data.KeyValues[string(originalKey)] = string(value)

		iter.Next()
	}

	pm.logger.Info("Exporting partition data",
		zap.Uint32("partition_id", partitionID),
		zap.Int("key_count", len(data.KeyValues)),
	)

	return json.Marshal(data)
}

// ImportPartitionData imports data into a partition from JSON bytes
func (pm *PartitionManager) ImportPartitionData(partitionID uint32, jsonData []byte) (int64, error) {
	// Ensure partition exists (add if not)
	pm.mu.Lock()
	if _, exists := pm.partitions[partitionID]; !exists {
		pm.partitions[partitionID] = &Partition{
			ID:      partitionID,
			Status:  pb.PartitionStatus_MIGRATING_IN,
			IsPrime: false,
		}
		common.PartitionGauge.WithLabelValues(pb.PartitionStatus_MIGRATING_IN.String()).Inc()
	}
	pm.mu.Unlock()

	var data PartitionData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return 0, fmt.Errorf("failed to unmarshal partition data: %w", err)
	}

	// Verify partition ID matches
	if data.PartitionID != partitionID {
		return 0, fmt.Errorf("partition ID mismatch: expected %d, got %d", partitionID, data.PartitionID)
	}

	// Import all key-value pairs
	items := make([]KeyValueItem, 0, len(data.KeyValues))
	for key, value := range data.KeyValues {
		storageKey := MakePartitionKey(partitionID, []byte(key))
		items = append(items, KeyValueItem{
			Key:   storageKey,
			Value: []byte(value),
		})
	}

	if len(items) > 0 {
		if err := pm.db.BatchPut(items); err != nil {
			return 0, fmt.Errorf("failed to batch put: %w", err)
		}
	}

	pm.logger.Info("Imported partition data",
		zap.Uint32("partition_id", partitionID),
		zap.Int("key_count", len(items)),
	)

	return int64(len(items)), nil
}

// ClearPartitionData removes all data for a partition
func (pm *PartitionManager) ClearPartitionData(partitionID uint32) error {
	startKey, endKey := GetPartitionRange(partitionID)

	// Iterate and delete all keys in range
	iter := pm.db.NewIterator()
	defer iter.Close()

	var keysToDelete [][]byte
	iter.Seek(startKey)
	for iter.Valid() {
		key := iter.Key().Data()

		if len(key) >= len(endKey) {
			keyPrefix := key[:len(endKey)]
			if string(keyPrefix) >= string(endKey) {
				break
			}
		}

		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keysToDelete = append(keysToDelete, keyCopy)

		iter.Next()
	}

	for _, key := range keysToDelete {
		if err := pm.db.Delete(key); err != nil {
			return fmt.Errorf("failed to delete key: %w", err)
		}
	}

	pm.logger.Info("Cleared partition data",
		zap.Uint32("partition_id", partitionID),
		zap.Int("keys_deleted", len(keysToDelete)),
	)

	return nil
}
