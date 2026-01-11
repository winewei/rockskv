package storage

import (
	"bytes"
	"testing"
)

// Test FieldBatch encode/decode with version byte
func TestFieldBatchVersion(t *testing.T) {
	// Create a field batch
	fb := &FieldBatch{
		PartitionID: 100,
		PrimaryKey:  []byte("user:123"),
		Updates: []FieldUpdate{
			{FieldName: "name", Value: []byte("Alice"), IsDelete: false},
			{FieldName: "age", Value: []byte("30"), IsDelete: false},
			{FieldName: "email", Value: nil, IsDelete: true},
		},
	}

	// Encode
	encoded := fb.Encode()

	// Verify version byte is first
	if encoded[0] != FieldBatchFormatVersion {
		t.Errorf("Expected version byte %d, got %d", FieldBatchFormatVersion, encoded[0])
	}

	// Decode
	decoded, err := DecodeFieldBatch(encoded)
	if err != nil {
		t.Fatalf("DecodeFieldBatch failed: %v", err)
	}

	// Verify fields
	if decoded.PartitionID != fb.PartitionID {
		t.Errorf("PartitionID mismatch: expected %d, got %d", fb.PartitionID, decoded.PartitionID)
	}
	if !bytes.Equal(decoded.PrimaryKey, fb.PrimaryKey) {
		t.Errorf("PrimaryKey mismatch: expected %v, got %v", fb.PrimaryKey, decoded.PrimaryKey)
	}
	if len(decoded.Updates) != len(fb.Updates) {
		t.Errorf("Updates count mismatch: expected %d, got %d", len(fb.Updates), len(decoded.Updates))
	}

	for i, update := range fb.Updates {
		if decoded.Updates[i].FieldName != update.FieldName {
			t.Errorf("Update[%d] FieldName mismatch: expected %s, got %s", i, update.FieldName, decoded.Updates[i].FieldName)
		}
		if !bytes.Equal(decoded.Updates[i].Value, update.Value) {
			t.Errorf("Update[%d] Value mismatch: expected %v, got %v", i, update.Value, decoded.Updates[i].Value)
		}
		if decoded.Updates[i].IsDelete != update.IsDelete {
			t.Errorf("Update[%d] IsDelete mismatch: expected %v, got %v", i, update.IsDelete, decoded.Updates[i].IsDelete)
		}
	}
}

// Test FieldBatch with empty updates
func TestFieldBatchEmptyUpdates(t *testing.T) {
	fb := &FieldBatch{
		PartitionID: 1,
		PrimaryKey:  []byte("key"),
		Updates:     []FieldUpdate{},
	}

	encoded := fb.Encode()
	decoded, err := DecodeFieldBatch(encoded)
	if err != nil {
		t.Fatalf("DecodeFieldBatch failed: %v", err)
	}

	if len(decoded.Updates) != 0 {
		t.Errorf("Expected 0 updates, got %d", len(decoded.Updates))
	}
}

// Test FieldKey encoding/decoding
func TestFieldKeyEncoding(t *testing.T) {
	testCases := []struct {
		name        string
		partitionID uint32
		pk          []byte
		fieldName   string
	}{
		{"basic", 100, []byte("user:1"), "name"},
		{"empty_pk", 0, []byte{}, "field"},
		{"binary_pk", 500, []byte{0x00, 0xFF, 0x01, '#', ':', 'f'}, "data"},
		{"unicode_field", 1000, []byte("key"), "名前"},
		{"max_partition", 4095, []byte("k"), "f"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fk := &FieldKey{
				PartitionID: tc.partitionID,
				PrimaryKey:  tc.pk,
				FieldName:   tc.fieldName,
			}

			encoded := fk.Encode()
			decoded, err := DecodeFieldKey(encoded)
			if err != nil {
				t.Fatalf("DecodeFieldKey failed: %v", err)
			}

			if decoded.PartitionID != tc.partitionID {
				t.Errorf("PartitionID mismatch: expected %d, got %d", tc.partitionID, decoded.PartitionID)
			}
			if !bytes.Equal(decoded.PrimaryKey, tc.pk) {
				t.Errorf("PrimaryKey mismatch: expected %v, got %v", tc.pk, decoded.PrimaryKey)
			}
			if decoded.FieldName != tc.fieldName {
				t.Errorf("FieldName mismatch: expected %s, got %s", tc.fieldName, decoded.FieldName)
			}
		})
	}
}

// Test FieldKey with primary key containing special characters
func TestFieldKeyWithSpecialCharacters(t *testing.T) {
	// Primary key containing characters that were problematic before (# and :)
	fk := &FieldKey{
		PartitionID: 123,
		PrimaryKey:  []byte("user#id:123"),
		FieldName:   "profile",
	}

	encoded := fk.Encode()
	decoded, err := DecodeFieldKey(encoded)
	if err != nil {
		t.Fatalf("DecodeFieldKey failed for special characters: %v", err)
	}

	if !bytes.Equal(decoded.PrimaryKey, fk.PrimaryKey) {
		t.Errorf("PrimaryKey with special chars mismatch: expected %v, got %v", fk.PrimaryKey, decoded.PrimaryKey)
	}
}

// Test ParsePartitionKeyEx for field key detection
func TestParsePartitionKeyEx(t *testing.T) {
	// Regular KV key
	partitionID := uint32(100)
	regularKey := MakePartitionKey(partitionID, []byte("mykey"))

	pID, key, isField, err := ParsePartitionKeyEx(regularKey)
	if err != nil {
		t.Fatalf("ParsePartitionKeyEx failed for regular key: %v", err)
	}
	if pID != partitionID {
		t.Errorf("PartitionID mismatch: expected %d, got %d", partitionID, pID)
	}
	if isField {
		t.Error("Expected regular key to not be detected as field key")
	}
	if string(key) != "mykey" {
		t.Errorf("Key mismatch: expected 'mykey', got '%s'", string(key))
	}

	// Field key
	fieldKey := MakeFieldKey(partitionID, []byte("pk"), "fieldName")
	pID, _, isField, err = ParsePartitionKeyEx(fieldKey)
	if err != nil {
		t.Fatalf("ParsePartitionKeyEx failed for field key: %v", err)
	}
	if pID != partitionID {
		t.Errorf("PartitionID mismatch: expected %d, got %d", partitionID, pID)
	}
	if !isField {
		t.Error("Expected field key to be detected as field key")
	}
}

// Test that field keys and regular keys don't overlap in range scan
func TestPartitionRangeIncludesFieldKeys(t *testing.T) {
	partitionID := uint32(100)

	// Get partition range
	startKey, endKey := GetPartitionRange(partitionID)

	// Create a field key
	fieldKey := MakeFieldKey(partitionID, []byte("pk"), "field")

	// Field key should be within partition range
	if bytes.Compare(fieldKey, startKey) < 0 {
		t.Error("Field key is before partition start")
	}
	if bytes.Compare(fieldKey, endKey) >= 0 {
		t.Error("Field key is after partition end")
	}
}

// Test DecodeFieldKey error cases
func TestDecodeFieldKeyErrors(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too_short", []byte{0x00, 0x01}},
		{"wrong_prefix", []byte("x:1234:fkey")},
		{"no_field_marker", []byte("p:\x00\x00\x00\x64key")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeFieldKey(tc.data)
			if err == nil {
				t.Error("Expected error for malformed field key")
			}
		})
	}
}

// Test GetFieldPrefix returns correct prefix for iteration
func TestGetFieldPrefix(t *testing.T) {
	partitionID := uint32(100)
	pk := []byte("user:123")

	prefix := GetFieldPrefix(partitionID, pk)

	// Verify prefix can be used to find matching field keys
	fieldKey1 := MakeFieldKey(partitionID, pk, "name")
	fieldKey2 := MakeFieldKey(partitionID, pk, "age")
	otherPKKey := MakeFieldKey(partitionID, []byte("user:999"), "name")

	if !bytes.HasPrefix(fieldKey1, prefix) {
		t.Error("Field key 1 should have prefix")
	}
	if !bytes.HasPrefix(fieldKey2, prefix) {
		t.Error("Field key 2 should have prefix")
	}
	if bytes.HasPrefix(otherPKKey, prefix) {
		t.Error("Field key with different PK should not have prefix")
	}
}

// Test GetPartitionFieldPrefix returns correct prefix for all field keys
func TestGetPartitionFieldPrefix(t *testing.T) {
	partitionID := uint32(100)

	prefix := GetPartitionFieldPrefix(partitionID)

	// All field keys in partition should have this prefix
	fieldKey1 := MakeFieldKey(partitionID, []byte("pk1"), "field")
	fieldKey2 := MakeFieldKey(partitionID, []byte("pk2"), "field")
	otherPartitionKey := MakeFieldKey(partitionID+1, []byte("pk1"), "field")

	if !bytes.HasPrefix(fieldKey1, prefix) {
		t.Error("Field key 1 should have partition field prefix")
	}
	if !bytes.HasPrefix(fieldKey2, prefix) {
		t.Error("Field key 2 should have partition field prefix")
	}
	if bytes.HasPrefix(otherPartitionKey, prefix) {
		t.Error("Field key from other partition should not have this prefix")
	}
}
