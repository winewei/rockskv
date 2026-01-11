//go:build cgo && !nocgo
// +build cgo,!nocgo

package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
)

// PatchOperation represents a single patch operation
type PatchOperation struct {
	Op    PatchOpType `json:"op"`    // Operation type
	Path  string      `json:"path"`  // Field path (e.g., "user.name", "items[0].price")
	Value []byte      `json:"value"` // New value (empty for delete)
}

// PatchOpType defines the type of patch operation
type PatchOpType uint8

const (
	// PatchOpSet sets a field value
	PatchOpSet PatchOpType = 1
	// PatchOpDelete deletes a field
	PatchOpDelete PatchOpType = 2
	// PatchOpIncr increments a numeric field
	PatchOpIncr PatchOpType = 3
	// PatchOpAppend appends to an array field
	PatchOpAppend PatchOpType = 4
)

// Patch represents a set of changes to apply to a value
type Patch struct {
	Operations []PatchOperation
}

// NewPatch creates a new patch
func NewPatch() *Patch {
	return &Patch{
		Operations: make([]PatchOperation, 0),
	}
}

// Set adds a set operation to the patch
func (p *Patch) Set(path string, value []byte) *Patch {
	p.Operations = append(p.Operations, PatchOperation{
		Op:    PatchOpSet,
		Path:  path,
		Value: value,
	})
	return p
}

// Delete adds a delete operation to the patch
func (p *Patch) Delete(path string) *Patch {
	p.Operations = append(p.Operations, PatchOperation{
		Op:   PatchOpDelete,
		Path: path,
	})
	return p
}

// Incr adds an increment operation to the patch
func (p *Patch) Incr(path string, delta int64) *Patch {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(delta))
	p.Operations = append(p.Operations, PatchOperation{
		Op:    PatchOpIncr,
		Path:  path,
		Value: value,
	})
	return p
}

// Append adds an append operation to the patch
func (p *Patch) Append(path string, value []byte) *Patch {
	p.Operations = append(p.Operations, PatchOperation{
		Op:    PatchOpAppend,
		Path:  path,
		Value: value,
	})
	return p
}

// Encode serializes the patch to bytes
// Format: count(4) + [op(1) + pathLen(2) + path + valueLen(4) + value]...
func (p *Patch) Encode() []byte {
	buf := new(bytes.Buffer)

	// Write operation count
	binary.Write(buf, binary.BigEndian, uint32(len(p.Operations)))

	for _, op := range p.Operations {
		// Write operation type
		buf.WriteByte(byte(op.Op))

		// Write path
		binary.Write(buf, binary.BigEndian, uint16(len(op.Path)))
		buf.WriteString(op.Path)

		// Write value
		binary.Write(buf, binary.BigEndian, uint32(len(op.Value)))
		buf.Write(op.Value)
	}

	return buf.Bytes()
}

// DecodePatch deserializes a patch from bytes
func DecodePatch(data []byte) (*Patch, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("patch data too short")
	}

	buf := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	p := &Patch{
		Operations: make([]PatchOperation, 0, count),
	}

	for i := uint32(0); i < count; i++ {
		var op PatchOperation

		// Read operation type
		opByte, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		op.Op = PatchOpType(opByte)

		// Read path
		var pathLen uint16
		if err := binary.Read(buf, binary.BigEndian, &pathLen); err != nil {
			return nil, err
		}
		pathBytes := make([]byte, pathLen)
		if _, err := buf.Read(pathBytes); err != nil {
			return nil, err
		}
		op.Path = string(pathBytes)

		// Read value
		var valueLen uint32
		if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}
		if valueLen > 0 {
			op.Value = make([]byte, valueLen)
			if _, err := buf.Read(op.Value); err != nil {
				return nil, err
			}
		}

		p.Operations = append(p.Operations, op)
	}

	return p, nil
}

// Apply applies the patch to a JSON value and returns the result
func (p *Patch) Apply(original []byte) ([]byte, error) {
	// Parse original as JSON
	var data map[string]interface{}
	if len(original) > 0 {
		if err := json.Unmarshal(original, &data); err != nil {
			// Try as raw value
			return nil, fmt.Errorf("original value is not valid JSON: %w", err)
		}
	} else {
		data = make(map[string]interface{})
	}

	// Apply each operation
	for _, op := range p.Operations {
		switch op.Op {
		case PatchOpSet:
			if err := setPath(data, op.Path, op.Value); err != nil {
				return nil, fmt.Errorf("set %s: %w", op.Path, err)
			}

		case PatchOpDelete:
			deletePath(data, op.Path)

		case PatchOpIncr:
			if err := incrPath(data, op.Path, op.Value); err != nil {
				return nil, fmt.Errorf("incr %s: %w", op.Path, err)
			}

		case PatchOpAppend:
			if err := appendPath(data, op.Path, op.Value); err != nil {
				return nil, fmt.Errorf("append %s: %w", op.Path, err)
			}
		}
	}

	return json.Marshal(data)
}

// setPath sets a value at the given path
func setPath(data map[string]interface{}, path string, value []byte) error {
	// Parse the value as JSON
	var jsonValue interface{}
	if err := json.Unmarshal(value, &jsonValue); err != nil {
		// Use as raw string
		jsonValue = string(value)
	}

	// Simple path for now (no nested paths)
	// TODO: Support nested paths like "user.name" and "items[0].price"
	data[path] = jsonValue
	return nil
}

// deletePath deletes a value at the given path
func deletePath(data map[string]interface{}, path string) {
	delete(data, path)
}

// incrPath increments a numeric value at the given path
func incrPath(data map[string]interface{}, path string, delta []byte) error {
	deltaVal := int64(binary.BigEndian.Uint64(delta))

	current, exists := data[path]
	if !exists {
		data[path] = float64(deltaVal)
		return nil
	}

	switch v := current.(type) {
	case float64:
		data[path] = v + float64(deltaVal)
	case int64:
		data[path] = v + deltaVal
	case int:
		data[path] = int64(v) + deltaVal
	default:
		return fmt.Errorf("cannot increment non-numeric field")
	}

	return nil
}

// appendPath appends a value to an array at the given path
func appendPath(data map[string]interface{}, path string, value []byte) error {
	// Parse the value as JSON
	var jsonValue interface{}
	if err := json.Unmarshal(value, &jsonValue); err != nil {
		jsonValue = string(value)
	}

	current, exists := data[path]
	if !exists {
		data[path] = []interface{}{jsonValue}
		return nil
	}

	arr, ok := current.([]interface{})
	if !ok {
		return fmt.Errorf("cannot append to non-array field")
	}

	data[path] = append(arr, jsonValue)
	return nil
}

// MergePatches merges multiple patches into one optimized patch
// Later operations override earlier ones for the same path
func MergePatches(patches ...*Patch) *Patch {
	// Group operations by path, keeping the last operation for each path
	pathOps := make(map[string]PatchOperation)
	pathOrder := make([]string, 0)

	for _, patch := range patches {
		for _, op := range patch.Operations {
			if _, exists := pathOps[op.Path]; !exists {
				pathOrder = append(pathOrder, op.Path)
			}
			pathOps[op.Path] = op
		}
	}

	// Sort paths for deterministic output
	sort.Strings(pathOrder)

	result := NewPatch()
	for _, path := range pathOrder {
		result.Operations = append(result.Operations, pathOps[path])
	}

	return result
}

// PatchSize returns the approximate size of the patch in bytes
func (p *Patch) PatchSize() int {
	size := 4 // count
	for _, op := range p.Operations {
		size += 1                // op type
		size += 2 + len(op.Path) // path
		size += 4 + len(op.Value) // value
	}
	return size
}
