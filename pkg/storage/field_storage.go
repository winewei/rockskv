//go:build cgo && !nocgo
// +build cgo,!nocgo

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
// Format: pk_len(4) + pk + separator(1) + field_name
// Example: user:123#name -> "Alice"
//          user:123#age -> 30
//          user:123#_meta -> {"created_at": ...}  // special group for metadata

const (
	// FieldSeparator separates primary key from field name
	FieldSeparator byte = '#'

	// MetaFieldName stores document metadata (schema version, created_at, etc.)
	MetaFieldName = "_meta"

	// AllFieldsName is a special field that stores all fields as JSON (for full doc read)
	AllFieldsName = "_all"
)

// FieldKey represents a key with field-level storage
type FieldKey struct {
	PrimaryKey []byte
	FieldName  string
}

// Encode creates the storage key for a field
// Format: pk + separator + field_name
func (fk *FieldKey) Encode() []byte {
	// pk#field_name
	buf := make([]byte, len(fk.PrimaryKey)+1+len(fk.FieldName))
	copy(buf, fk.PrimaryKey)
	buf[len(fk.PrimaryKey)] = FieldSeparator
	copy(buf[len(fk.PrimaryKey)+1:], fk.FieldName)
	return buf
}

// DecodeFieldKey parses a storage key into FieldKey
func DecodeFieldKey(key []byte) (*FieldKey, error) {
	idx := bytes.IndexByte(key, FieldSeparator)
	if idx < 0 {
		return nil, fmt.Errorf("invalid field key: no separator")
	}
	return &FieldKey{
		PrimaryKey: key[:idx],
		FieldName:  string(key[idx+1:]),
	}, nil
}

// MakeFieldKey creates a storage key for a specific field
func MakeFieldKey(pk []byte, fieldName string) []byte {
	fk := &FieldKey{PrimaryKey: pk, FieldName: fieldName}
	return fk.Encode()
}

// GetFieldPrefix returns the prefix for all fields of a primary key
func GetFieldPrefix(pk []byte) []byte {
	buf := make([]byte, len(pk)+1)
	copy(buf, pk)
	buf[len(pk)] = FieldSeparator
	return buf
}

// FieldUpdate represents an update to a single field
type FieldUpdate struct {
	FieldName string
	Value     []byte
	IsDelete  bool
}

// FieldBatch represents a batch of field updates for a single document
type FieldBatch struct {
	PrimaryKey []byte
	Updates    []FieldUpdate
}

// Encode serializes the field batch for command log
// Format: pk_len(4) + pk + update_count(4) + [field_len(2) + field + is_delete(1) + value_len(4) + value]...
func (fb *FieldBatch) Encode() []byte {
	buf := new(bytes.Buffer)

	// Write primary key
	binary.Write(buf, binary.BigEndian, uint32(len(fb.PrimaryKey)))
	buf.Write(fb.PrimaryKey)

	// Write update count
	binary.Write(buf, binary.BigEndian, uint32(len(fb.Updates)))

	for _, update := range fb.Updates {
		// Write field name
		binary.Write(buf, binary.BigEndian, uint16(len(update.FieldName)))
		buf.WriteString(update.FieldName)

		// Write is_delete flag
		if update.IsDelete {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}

		// Write value
		binary.Write(buf, binary.BigEndian, uint32(len(update.Value)))
		buf.Write(update.Value)
	}

	return buf.Bytes()
}

// DecodeFieldBatch deserializes a field batch from bytes
func DecodeFieldBatch(data []byte) (*FieldBatch, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("field batch data too short")
	}

	buf := bytes.NewReader(data)

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
		PrimaryKey: pk,
		Updates:    make([]FieldUpdate, 0, count),
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
	db *RocksDB
}

// NewFieldStorage creates a new field storage wrapper
func NewFieldStorage(db *RocksDB) *FieldStorage {
	return &FieldStorage{db: db}
}

// GetField retrieves a single field
func (fs *FieldStorage) GetField(pk []byte, fieldName string) ([]byte, bool, error) {
	key := MakeFieldKey(pk, fieldName)
	return fs.db.Get(key)
}

// SetField sets a single field
func (fs *FieldStorage) SetField(pk []byte, fieldName string, value []byte) error {
	key := MakeFieldKey(pk, fieldName)
	return fs.db.Put(key, value)
}

// DeleteField deletes a single field
func (fs *FieldStorage) DeleteField(pk []byte, fieldName string) error {
	key := MakeFieldKey(pk, fieldName)
	return fs.db.Delete(key)
}

// GetAllFields retrieves all fields for a primary key
func (fs *FieldStorage) GetAllFields(pk []byte) (map[string][]byte, error) {
	prefix := GetFieldPrefix(pk)
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

		// Extract field name
		fieldName := string(key.Data()[len(prefix):])
		value := iter.Value()

		// Copy value since iterator reuses memory
		valueCopy := make([]byte, len(value.Data()))
		copy(valueCopy, value.Data())

		fields[fieldName] = valueCopy

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
		key := MakeFieldKey(pk, fieldName)
		batch.Put(key, value)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return fs.db.db.Write(wo, batch)
}

// ApplyFieldBatch applies a batch of field updates
func (fs *FieldStorage) ApplyFieldBatch(fb *FieldBatch) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, update := range fb.Updates {
		key := MakeFieldKey(fb.PrimaryKey, update.FieldName)
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
	prefix := GetFieldPrefix(pk)

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

// ListPrimaryKeys returns all unique primary keys (for debugging/admin)
func (fs *FieldStorage) ListPrimaryKeys(prefix []byte, limit int) ([][]byte, error) {
	pks := make([][]byte, 0)
	seen := make(map[string]bool)

	iter := fs.db.NewIterator()
	defer iter.Close()

	for iter.Seek(prefix); iter.Valid() && len(pks) < limit; iter.Next() {
		key := iter.Key()

		// Find separator
		idx := bytes.IndexByte(key.Data(), FieldSeparator)
		if idx < 0 {
			key.Free()
			continue
		}

		pk := string(key.Data()[:idx])
		if !seen[pk] {
			seen[pk] = true
			pkCopy := make([]byte, idx)
			copy(pkCopy, key.Data()[:idx])
			pks = append(pks, pkCopy)
		}

		key.Free()
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
	binary.Write(buf, binary.BigEndian, uint32(len(ag.Fields)))

	for _, name := range names {
		value := ag.Fields[name]
		binary.Write(buf, binary.BigEndian, uint16(len(name)))
		buf.WriteString(name)
		binary.Write(buf, binary.BigEndian, uint32(len(value)))
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
// Key format: pk#group_name -> encoded attribute group
type GroupedFieldStorage struct {
	db *RocksDB
}

// NewGroupedFieldStorage creates a new grouped field storage
func NewGroupedFieldStorage(db *RocksDB) *GroupedFieldStorage {
	return &GroupedFieldStorage{db: db}
}

// GetGroup retrieves an attribute group
func (gfs *GroupedFieldStorage) GetGroup(pk []byte, groupName string) (*AttributeGroup, error) {
	key := MakeFieldKey(pk, groupName)
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
	key := MakeFieldKey(pk, group.Name)
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
	key := MakeFieldKey(pk, groupName)
	return gfs.db.Delete(key)
}
