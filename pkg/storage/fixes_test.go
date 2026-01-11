package storage

import (
	"bytes"
	"strings"
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

// Test CommandEntry encode/decode round-trip (regression test)
func TestCommandEntryEncodeDecodeRoundTrip(t *testing.T) {
	entry := &CommandEntry{
		Sequence:  12345,
		Timestamp: 1234567890,
		CmdType:   CmdTypePut,
		KeyLen:    5,
		ValueLen:  6,
		Key:       []byte("mykey"),
		Value:     []byte("mydata"),
	}

	// Encode
	encoded := entry.Encode()

	// Decode
	reader := bytes.NewReader(encoded)
	decoded, err := DecodeCommandEntry(reader)
	if err != nil {
		t.Fatalf("DecodeCommandEntry failed: %v", err)
	}

	// Verify all fields match
	if decoded.Sequence != entry.Sequence {
		t.Errorf("Sequence mismatch: expected %d, got %d", entry.Sequence, decoded.Sequence)
	}
	if decoded.Timestamp != entry.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", entry.Timestamp, decoded.Timestamp)
	}
	if decoded.CmdType != entry.CmdType {
		t.Errorf("CmdType mismatch: expected %d, got %d", entry.CmdType, decoded.CmdType)
	}
	if !bytes.Equal(decoded.Key, entry.Key) {
		t.Errorf("Key mismatch: expected %v, got %v", entry.Key, decoded.Key)
	}
	if !bytes.Equal(decoded.Value, entry.Value) {
		t.Errorf("Value mismatch: expected %v, got %v", entry.Value, decoded.Value)
	}
}

// Test CRC mismatch detection with corrupted byte
func TestCommandEntryCRCMismatch(t *testing.T) {
	entry := &CommandEntry{
		Sequence:  100,
		Timestamp: 9999999,
		CmdType:   CmdTypePut,
		KeyLen:    4,
		ValueLen:  5,
		Key:       []byte("test"),
		Value:     []byte("value"),
	}

	// Encode valid entry
	encoded := entry.Encode()

	// Corrupt a byte in the value section (flip a bit)
	// Value starts at: 8 (seq) + 8 (ts) + 1 (type) + 4 (keyLen) + 4 (valueLen) + 4 (key) = 29
	valueOffset := 8 + 8 + 1 + 4 + 4 + len(entry.Key)
	encoded[valueOffset] ^= 0xFF // Flip all bits of first value byte

	// Decode should fail with CRC mismatch
	reader := bytes.NewReader(encoded)
	_, err := DecodeCommandEntry(reader)
	if err == nil {
		t.Fatal("Expected CRC mismatch error for corrupted entry, got nil")
	}
	if !strings.Contains(err.Error(), "CRC mismatch") {
		t.Errorf("Expected CRC mismatch error, got: %v", err)
	}
}

// Test truncated log - missing CRC bytes
func TestCommandEntryTruncatedCRC(t *testing.T) {
	entry := &CommandEntry{
		Sequence:  200,
		Timestamp: 1111111,
		CmdType:   CmdTypeDelete,
		KeyLen:    3,
		ValueLen:  0,
		Key:       []byte("abc"),
		Value:     nil,
	}

	// Encode valid entry
	encoded := entry.Encode()

	// Truncate: remove last 4 bytes (CRC)
	truncated := encoded[:len(encoded)-4]

	reader := bytes.NewReader(truncated)
	_, err := DecodeCommandEntry(reader)
	if err == nil {
		t.Fatal("Expected error for truncated CRC, got nil")
	}
}

// Test truncated log - missing value bytes
func TestCommandEntryTruncatedValue(t *testing.T) {
	entry := &CommandEntry{
		Sequence:  300,
		Timestamp: 2222222,
		CmdType:   CmdTypePut,
		KeyLen:    4,
		ValueLen:  10,
		Key:       []byte("key1"),
		Value:     []byte("0123456789"),
	}

	// Encode valid entry
	encoded := entry.Encode()

	// Truncate: remove half of value + CRC (keeping header + key + 5 bytes of value)
	// header = 25 bytes, key = 4 bytes, value should be 10 bytes
	truncated := encoded[:25+4+5] // Missing 5 value bytes and 4 CRC bytes

	reader := bytes.NewReader(truncated)
	_, err := DecodeCommandEntry(reader)
	if err == nil {
		t.Fatal("Expected error for truncated value, got nil")
	}
}

// Test truncated log - missing header bytes
func TestCommandEntryTruncatedHeader(t *testing.T) {
	entry := &CommandEntry{
		Sequence:  400,
		Timestamp: 3333333,
		CmdType:   CmdTypePut,
		KeyLen:    2,
		ValueLen:  3,
		Key:       []byte("ab"),
		Value:     []byte("xyz"),
	}

	// Encode valid entry
	encoded := entry.Encode()

	// Truncate to just 10 bytes (partial header)
	truncated := encoded[:10]

	reader := bytes.NewReader(truncated)
	_, err := DecodeCommandEntry(reader)
	if err == nil {
		t.Fatal("Expected error for truncated header, got nil")
	}
}

// Test ComputeEntryCRC is consistent with Encode
func TestComputeEntryCRCConsistency(t *testing.T) {
	entry := &CommandEntry{
		Sequence:  500,
		Timestamp: 4444444,
		CmdType:   CmdTypePut,
		KeyLen:    6,
		ValueLen:  7,
		Key:       []byte("keysix"),
		Value:     []byte("val7777"),
	}

	// Encode (which calculates CRC internally)
	encoded := entry.Encode()

	// Build header manually to match what DecodeCommandEntry reads
	header := make([]byte, 8+8+1+4+4)
	copy(header, encoded[:25])

	// Compute CRC using pure function
	computedCRC := ComputeEntryCRC(header, entry.Key, entry.Value)

	// Entry's CRC should match computed CRC
	if entry.CRC != computedCRC {
		t.Errorf("CRC mismatch: Encode set %x, ComputeEntryCRC returned %x", entry.CRC, computedCRC)
	}
}
