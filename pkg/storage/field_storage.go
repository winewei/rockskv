
package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/linxGnu/grocksdb"
)

// Field storage key format:
// Format: "p:" + partition_id(4 bytes) + ":" + pk_len(4 bytes) + pk + field_name
// The partition prefix ensures field data is co-located with regular KV data
// and migrated together during partition migration.

const (
	// FieldKeyMarker marks a field key (distinguishes from regular KV keys)
	FieldKeyMarker byte = 'f'

	// MetaFieldName stores document metadata (schema version, created_at, etc.)
	MetaFieldName = "_meta"

	// AllFieldsName is a special field that stores all fields as JSON (for full doc read)
	AllFieldsName = "_all"
)

// FieldKey represents a key with field-level storage
type FieldKey struct {
	PartitionID uint32
	PrimaryKey  []byte
	FieldName   string
}

// Encode creates the storage key for a field
// Format: "p:" + partition_id(4) + ":f" + pk_len(4) + pk + field_name
// This format:
// - Starts with partition prefix for data isolation
// - Uses 'f' marker to distinguish from regular keys
// - Uses length-prefix for pk to handle any byte sequence safely
func (fk *FieldKey) Encode() []byte {
	// p:<partition_id>:f<pk_len><pk><field_name>
	// Total: 2 + 4 + 1 + 1 + 4 + len(pk) + len(field_name)
	buf := make([]byte, 2+4+1+1+4+len(fk.PrimaryKey)+len(fk.FieldName))
	buf[0] = 'p'
	buf[1] = ':'
	binary.BigEndian.PutUint32(buf[2:6], fk.PartitionID)
	buf[6] = ':'
	buf[7] = FieldKeyMarker
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(fk.PrimaryKey)))
	copy(buf[12:], fk.PrimaryKey)
	copy(buf[12+len(fk.PrimaryKey):], fk.FieldName)
	return buf
}

// DecodeFieldKey parses a storage key into FieldKey
func DecodeFieldKey(key []byte) (*FieldKey, error) {
	// Minimum length: p: + partition(4) + :f + pk_len(4) + at least 0 bytes pk
	if len(key) < 12 {
		return nil, fmt.Errorf("invalid field key: too short")
	}
	if key[0] != 'p' || key[1] != ':' || key[6] != ':' || key[7] != FieldKeyMarker {
		return nil, fmt.Errorf("invalid field key: wrong format")
	}

	partitionID := binary.BigEndian.Uint32(key[2:6])
	pkLen := binary.BigEndian.Uint32(key[8:12])

	if len(key) < int(12+pkLen) {
		return nil, fmt.Errorf("invalid field key: pk length mismatch")
	}

	return &FieldKey{
		PartitionID: partitionID,
		PrimaryKey:  key[12 : 12+pkLen],
		FieldName:   string(key[12+pkLen:]),
	}, nil
}

// MakeFieldKey creates a storage key for a specific field with partition
func MakeFieldKey(partitionID uint32, pk []byte, fieldName string) []byte {
	fk := &FieldKey{PartitionID: partitionID, PrimaryKey: pk, FieldName: fieldName}
	return fk.Encode()
}

// GetFieldPrefix returns the prefix for all fields of a primary key within a partition
func GetFieldPrefix(partitionID uint32, pk []byte) []byte {
	// p:<partition_id>:f<pk_len><pk>
	buf := make([]byte, 2+4+1+1+4+len(pk))
	buf[0] = 'p'
	buf[1] = ':'
	binary.BigEndian.PutUint32(buf[2:6], partitionID)
	buf[6] = ':'
	buf[7] = FieldKeyMarker
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(pk)))
	copy(buf[12:], pk)
	return buf
}

// GetPartitionFieldPrefix returns the prefix for all field keys in a partition
// Used during partition migration to export all field data
func GetPartitionFieldPrefix(partitionID uint32) []byte {
	// p:<partition_id>:f
	buf := make([]byte, 8)
	buf[0] = 'p'
	buf[1] = ':'
	binary.BigEndian.PutUint32(buf[2:6], partitionID)
	buf[6] = ':'
	buf[7] = FieldKeyMarker
	return buf
}

// FieldUpdate represents an update to a single field
type FieldUpdate struct {
	FieldName string
	Value     []byte
	IsDelete  bool
}

// FieldBatch format version for backward compatibility
const FieldBatchFormatVersion byte = 1

// FieldBatch represents a batch of field updates for a single document
type FieldBatch struct {
	PartitionID uint32 // Partition ID for replication context
	PrimaryKey  []byte
	Updates     []FieldUpdate
}

// Encode serializes the field batch for command log
// Format: version(1) + partition_id(4) + pk_len(4) + pk + update_count(4) + [field_len(2) + field + is_delete(1) + value_len(4) + value]...
func (fb *FieldBatch) Encode() []byte {
	buf := new(bytes.Buffer)

	// Write version byte for future compatibility
	buf.WriteByte(FieldBatchFormatVersion)

	// Write partition ID
	_ = binary.Write(buf, binary.BigEndian, fb.PartitionID) // bytes.Buffer never returns error

	// Write primary key
	_ = binary.Write(buf, binary.BigEndian, uint32(len(fb.PrimaryKey))) // bytes.Buffer never returns error
	buf.Write(fb.PrimaryKey)

	// Write update count
	_ = binary.Write(buf, binary.BigEndian, uint32(len(fb.Updates))) // bytes.Buffer never returns error

	for _, update := range fb.Updates {
		// Write field name
		_ = binary.Write(buf, binary.BigEndian, uint16(len(update.FieldName))) // bytes.Buffer never returns error
		buf.WriteString(update.FieldName)

		// Write is_delete flag
		if update.IsDelete {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}

		// Write value
		_ = binary.Write(buf, binary.BigEndian, uint32(len(update.Value))) // bytes.Buffer never returns error
		buf.Write(update.Value)
	}

	return buf.Bytes()
}

// DecodeFieldBatch deserializes a field batch from bytes
// Supports both versioned (v1+) and legacy (v0) formats for backward compatibility
func DecodeFieldBatch(data []byte) (*FieldBatch, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("field batch data too short")
	}

	// Check version byte
	version := data[0]

	switch version {
	case FieldBatchFormatVersion:
		return decodeFieldBatchV1(data[1:])
	default:
		// Assume legacy format (no version byte) if version byte looks like partition ID
		// Legacy format starts with partition_id(4), so first byte would be 0 for partition < 256
		// or higher values. Version 1 is 0x01.
		// If first 4 bytes could be a reasonable partition ID, treat as legacy
		if len(data) >= 12 {
			return decodeFieldBatchLegacy(data)
		}
		return nil, fmt.Errorf("unsupported field batch version: %d", version)
	}
}

// decodeFieldBatchV1 decodes version 1 format (after version byte)
func decodeFieldBatchV1(data []byte) (*FieldBatch, error) {
	if len(data) < 12 { // partition_id(4) + pk_len(4) + count(4)
		return nil, fmt.Errorf("field batch v1 data too short")
	}

	buf := bytes.NewReader(data)
	return decodeFieldBatchContent(buf)
}

// decodeFieldBatchLegacy decodes legacy format (no version byte)
func decodeFieldBatchLegacy(data []byte) (*FieldBatch, error) {
	buf := bytes.NewReader(data)
	return decodeFieldBatchContent(buf)
}

// decodeFieldBatchContent decodes the field batch content from a reader
func decodeFieldBatchContent(buf *bytes.Reader) (*FieldBatch, error) {
	// Read partition ID
	var partitionID uint32
	if err := binary.Read(buf, binary.BigEndian, &partitionID); err != nil {
		return nil, err
	}

	// Read primary key
	var pkLen uint32
	if err := binary.Read(buf, binary.BigEndian, &pkLen); err != nil {
		return nil, err
	}
	pk := make([]byte, pkLen)
	if _, err := buf.Read(pk); err != nil {
		return nil, err
	}

	// Read update count
	var count uint32
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	fb := &FieldBatch{
		PartitionID: partitionID,
		PrimaryKey:  pk,
		Updates:     make([]FieldUpdate, 0, count),
	}

	for i := uint32(0); i < count; i++ {
		var update FieldUpdate

		// Read field name
		var fieldLen uint16
		if err := binary.Read(buf, binary.BigEndian, &fieldLen); err != nil {
			return nil, err
		}
		fieldBytes := make([]byte, fieldLen)
		if _, err := buf.Read(fieldBytes); err != nil {
			return nil, err
		}
		update.FieldName = string(fieldBytes)

		// Read is_delete flag
		isDeleteByte, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		update.IsDelete = isDeleteByte == 1

		// Read value
		var valueLen uint32
		if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}
		if valueLen > 0 {
			update.Value = make([]byte, valueLen)
			if _, err := buf.Read(update.Value); err != nil {
				return nil, err
			}
		}

		fb.Updates = append(fb.Updates, update)
	}

	return fb, nil
}

// FieldStorage provides field-level storage operations
type FieldStorage struct {
	db          *RocksDB
	partitionID uint32
}

// NewFieldStorage creates a new field storage wrapper for a specific partition
func NewFieldStorage(db *RocksDB, partitionID uint32) *FieldStorage {
	return &FieldStorage{db: db, partitionID: partitionID}
}

// GetField retrieves a single field
func (fs *FieldStorage) GetField(pk []byte, fieldName string) ([]byte, bool, error) {
	key := MakeFieldKey(fs.partitionID, pk, fieldName)
	return fs.db.Get(key)
}

// SetField sets a single field
func (fs *FieldStorage) SetField(pk []byte, fieldName string, value []byte) error {
	key := MakeFieldKey(fs.partitionID, pk, fieldName)
	return fs.db.Put(key, value)
}

// DeleteField deletes a single field
func (fs *FieldStorage) DeleteField(pk []byte, fieldName string) error {
	key := MakeFieldKey(fs.partitionID, pk, fieldName)
	return fs.db.Delete(key)
}

// GetAllFields retrieves all fields for a primary key
func (fs *FieldStorage) GetAllFields(pk []byte) (map[string][]byte, error) {
	prefix := GetFieldPrefix(fs.partitionID, pk)
	fields := make(map[string][]byte)

	iter := fs.db.NewIterator()
	defer iter.Close()

	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Check if still within this PK's fields
		if !bytes.HasPrefix(key.Data(), prefix) {
			key.Free()
			break
		}

		// Validate and parse the complete field key to ensure correct format
		fk, err := DecodeFieldKey(key.Data())
		if err != nil {
			key.Free()
			continue // Skip malformed keys
		}

		// Verify partition ID and PK match (extra safety check)
		if fk.PartitionID != fs.partitionID || !bytes.Equal(fk.PrimaryKey, pk) {
			key.Free()
			continue
		}

		value := iter.Value()

		// Copy value since iterator reuses memory
		valueCopy := make([]byte, len(value.Data()))
		copy(valueCopy, value.Data())

		fields[fk.FieldName] = valueCopy

		key.Free()
		value.Free()
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return fields, nil
}

// SetFields sets multiple fields atomically
func (fs *FieldStorage) SetFields(pk []byte, fields map[string][]byte) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for fieldName, value := range fields {
		key := MakeFieldKey(fs.partitionID, pk, fieldName)
		batch.Put(key, value)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return fs.db.db.Write(wo, batch)
}

// ApplyFieldBatch applies a batch of field updates
// Note: partitionID in FieldBatch is used for replication context,
// storage uses fs.partitionID for key construction
func (fs *FieldStorage) ApplyFieldBatch(fb *FieldBatch) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, update := range fb.Updates {
		key := MakeFieldKey(fs.partitionID, fb.PrimaryKey, update.FieldName)
		if update.IsDelete {
			batch.Delete(key)
		} else {
			batch.Put(key, update.Value)
		}
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return fs.db.db.Write(wo, batch)
}

// DeleteAllFields deletes all fields for a primary key
func (fs *FieldStorage) DeleteAllFields(pk []byte) error {
	prefix := GetFieldPrefix(fs.partitionID, pk)

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	iter := fs.db.NewIterator()
	defer iter.Close()

	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key.Data(), prefix) {
			key.Free()
			break
		}

		batch.Delete(key.Data())
		key.Free()
	}

	if err := iter.Err(); err != nil {
		return err
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return fs.db.db.Write(wo, batch)
}

// ToJSON converts all fields to a JSON document
func (fs *FieldStorage) ToJSON(pk []byte) ([]byte, error) {
	fields, err := fs.GetAllFields(pk)
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		return nil, nil
	}

	// Convert fields to JSON object
	doc := make(map[string]interface{})
	for name, value := range fields {
		// Skip internal fields
		if name[0] == '_' {
			continue
		}

		// Try to parse as JSON
		var jsonValue interface{}
		if err := json.Unmarshal(value, &jsonValue); err != nil {
			// Use as raw string
			doc[name] = string(value)
		} else {
			doc[name] = jsonValue
		}
	}

	return json.Marshal(doc)
}

// FromJSON stores a JSON document as individual fields
func (fs *FieldStorage) FromJSON(pk []byte, data []byte) error {
	var doc map[string]interface{}
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	fields := make(map[string][]byte)
	for name, value := range doc {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal field %s: %w", name, err)
		}
		fields[name] = jsonValue
	}

	return fs.SetFields(pk, fields)
}

// ListPrimaryKeys returns all unique primary keys in this partition (for debugging/admin)
func (fs *FieldStorage) ListPrimaryKeys(limit int) ([][]byte, error) {
	pks := make([][]byte, 0)
	seen := make(map[string]bool)

	prefix := GetPartitionFieldPrefix(fs.partitionID)
	iter := fs.db.NewIterator()
	defer iter.Close()

	for iter.Seek(prefix); iter.Valid() && len(pks) < limit; iter.Next() {
		key := iter.Key()

		// Check if still within partition's field keys
		if !bytes.HasPrefix(key.Data(), prefix) {
			key.Free()
			break
		}

		// Parse the field key to extract pk
		fk, err := DecodeFieldKey(key.Data())
		key.Free()
		if err != nil {
			continue
		}

		pkStr := string(fk.PrimaryKey)
		if !seen[pkStr] {
			seen[pkStr] = true
			pkCopy := make([]byte, len(fk.PrimaryKey))
			copy(pkCopy, fk.PrimaryKey)
			pks = append(pks, pkCopy)
		}
	}

	return pks, iter.Err()
}

// AttributeGroup represents a group of related attributes stored together
type AttributeGroup struct {
	Name   string            // Group name (e.g., "profile", "settings")
	Fields map[string][]byte // Field name -> value
}

// EncodeGroup encodes an attribute group to bytes
func (ag *AttributeGroup) Encode() []byte {
	// Sort field names for deterministic output
	names := make([]string, 0, len(ag.Fields))
	for name := range ag.Fields {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(ag.Fields))) // bytes.Buffer never returns error

	for _, name := range names {
		value := ag.Fields[name]
		_ = binary.Write(buf, binary.BigEndian, uint16(len(name))) // bytes.Buffer never returns error
		buf.WriteString(name)
		_ = binary.Write(buf, binary.BigEndian, uint32(len(value))) // bytes.Buffer never returns error
		buf.Write(value)
	}

	return buf.Bytes()
}

// DecodeAttributeGroup decodes an attribute group from bytes
func DecodeAttributeGroup(data []byte) (*AttributeGroup, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("attribute group data too short")
	}

	buf := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	ag := &AttributeGroup{
		Fields: make(map[string][]byte),
	}

	for i := uint32(0); i < count; i++ {
		var nameLen uint16
		if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := buf.Read(nameBytes); err != nil {
			return nil, err
		}

		var valueLen uint32
		if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}
		value := make([]byte, valueLen)
		if _, err := buf.Read(value); err != nil {
			return nil, err
		}

		ag.Fields[string(nameBytes)] = value
	}

	return ag, nil
}

// GroupedFieldStorage provides attribute-group-based storage
// Key format: p:<partition_id>:f<pk_len><pk><group_name> -> encoded attribute group
type GroupedFieldStorage struct {
	db          *RocksDB
	partitionID uint32
}

// NewGroupedFieldStorage creates a new grouped field storage
func NewGroupedFieldStorage(db *RocksDB, partitionID uint32) *GroupedFieldStorage {
	return &GroupedFieldStorage{db: db, partitionID: partitionID}
}

// GetGroup retrieves an attribute group
func (gfs *GroupedFieldStorage) GetGroup(pk []byte, groupName string) (*AttributeGroup, error) {
	key := MakeFieldKey(gfs.partitionID, pk, groupName)
	data, found, err := gfs.db.Get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	group, err := DecodeAttributeGroup(data)
	if err != nil {
		return nil, err
	}
	group.Name = groupName
	return group, nil
}

// SetGroup stores an attribute group
func (gfs *GroupedFieldStorage) SetGroup(pk []byte, group *AttributeGroup) error {
	key := MakeFieldKey(gfs.partitionID, pk, group.Name)
	return gfs.db.Put(key, group.Encode())
}

// UpdateGroupFields updates specific fields within a group
func (gfs *GroupedFieldStorage) UpdateGroupFields(pk []byte, groupName string, updates map[string][]byte, deletes []string) error {
	// Get existing group
	group, err := gfs.GetGroup(pk, groupName)
	if err != nil {
		return err
	}
	if group == nil {
		group = &AttributeGroup{
			Name:   groupName,
			Fields: make(map[string][]byte),
		}
	}

	// Apply updates
	for name, value := range updates {
		group.Fields[name] = value
	}

	// Apply deletes
	for _, name := range deletes {
		delete(group.Fields, name)
	}

	// Write back
	return gfs.SetGroup(pk, group)
}

// DeleteGroup deletes an entire attribute group
func (gfs *GroupedFieldStorage) DeleteGroup(pk []byte, groupName string) error {
	key := MakeFieldKey(gfs.partitionID, pk, groupName)
	return gfs.db.Delete(key)
}
