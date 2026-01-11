//go:build cgo && !nocgo
// +build cgo,!nocgo

package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
)

const (
	// Command types
	CmdTypePut        uint8 = 1
	CmdTypeDelete     uint8 = 2
	CmdTypeFieldBatch uint8 = 3 // Field-level batch update

	// Log file settings
	LogFileMaxSize   = 64 * 1024 * 1024 // 64MB per log file
	LogFileSuffix    = ".cmdlog"
	LogIndexSuffix   = ".cmdidx"
	LogSyncInterval  = 100 * time.Millisecond
	LogBufferSize    = 4 * 1024 * 1024 // 4MB write buffer

	// Magic number for log file header
	LogMagic = 0x524B434C // "RKCL" - RocksKV Command Log
)

// CommandEntry represents a single command in the log
type CommandEntry struct {
	Sequence  uint64 // Monotonically increasing sequence number
	Timestamp int64  // Unix nano timestamp
	CmdType   uint8  // Put or Delete
	KeyLen    uint32
	ValueLen  uint32
	Key       []byte
	Value     []byte // Empty for Delete
	CRC       uint32 // CRC32 checksum
}

// Encode serializes the command entry to bytes
func (e *CommandEntry) Encode() []byte {
	// Format: seq(8) + ts(8) + type(1) + keyLen(4) + valueLen(4) + key + value + crc(4)
	size := 8 + 8 + 1 + 4 + 4 + len(e.Key) + len(e.Value) + 4
	buf := make([]byte, size)

	offset := 0
	binary.BigEndian.PutUint64(buf[offset:], e.Sequence)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.Timestamp))
	offset += 8
	buf[offset] = e.CmdType
	offset += 1
	binary.BigEndian.PutUint32(buf[offset:], e.KeyLen)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], e.ValueLen)
	offset += 4
	copy(buf[offset:], e.Key)
	offset += len(e.Key)
	copy(buf[offset:], e.Value)
	offset += len(e.Value)

	// Calculate CRC of everything except the CRC field itself
	e.CRC = crc32.ChecksumIEEE(buf[:offset])
	binary.BigEndian.PutUint32(buf[offset:], e.CRC)

	return buf
}

// DecodeCommandEntry deserializes a command entry from reader
func DecodeCommandEntry(r io.Reader) (*CommandEntry, error) {
	header := make([]byte, 8+8+1+4+4) // seq + ts + type + keyLen + valueLen
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	e := &CommandEntry{
		Sequence:  binary.BigEndian.Uint64(header[0:8]),
		Timestamp: int64(binary.BigEndian.Uint64(header[8:16])),
		CmdType:   header[16],
		KeyLen:    binary.BigEndian.Uint32(header[17:21]),
		ValueLen:  binary.BigEndian.Uint32(header[21:25]),
	}

	// Read key
	e.Key = make([]byte, e.KeyLen)
	if _, err := io.ReadFull(r, e.Key); err != nil {
		return nil, err
	}

	// Read value
	if e.ValueLen > 0 {
		e.Value = make([]byte, e.ValueLen)
		if _, err := io.ReadFull(r, e.Value); err != nil {
			return nil, err
		}
	}

	// Read and verify CRC
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, crcBuf); err != nil {
		return nil, err
	}
	e.CRC = binary.BigEndian.Uint32(crcBuf)

	// Verify CRC
	encoded := e.Encode()
	expectedCRC := binary.BigEndian.Uint32(encoded[len(encoded)-4:])
	if e.CRC != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch: got %x, expected %x", e.CRC, expectedCRC)
	}

	return e, nil
}

// CommandLog manages persistent command logs for a partition
type CommandLog struct {
	partitionID uint32
	logDir      string
	logger      *zap.Logger

	// Current write state
	currentFile   *os.File
	currentWriter *bufio.Writer
	currentSize   int64
	currentSeq    uint64

	// Sync control
	lastSyncSeq uint64
	syncTicker  *time.Ticker
	syncCh      chan struct{}

	// Index: sequence -> file offset (for fast lookup)
	index map[uint64]logPosition

	mu     sync.RWMutex
	closed atomic.Bool
}

type logPosition struct {
	fileNum int64
	offset  int64
}

// NewCommandLog creates a new command log for a partition
func NewCommandLog(partitionID uint32, logDir string) (*CommandLog, error) {
	dir := filepath.Join(logDir, fmt.Sprintf("partition_%d", partitionID))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	cl := &CommandLog{
		partitionID: partitionID,
		logDir:      dir,
		logger:      common.NewLogger("command-log"),
		index:       make(map[uint64]logPosition),
		syncCh:      make(chan struct{}, 1),
	}

	// Load existing logs and find latest sequence
	if err := cl.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover log: %w", err)
	}

	// Open current log file for writing
	if err := cl.openNewLogFile(); err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Start background sync
	cl.syncTicker = time.NewTicker(LogSyncInterval)
	go cl.syncLoop()

	cl.logger.Info("Command log initialized",
		zap.Uint32("partition_id", partitionID),
		zap.String("dir", dir),
		zap.Uint64("last_seq", cl.currentSeq),
	)

	return cl, nil
}

// Append adds a new command to the log
func (cl *CommandLog) Append(cmdType uint8, key, value []byte) (uint64, error) {
	if cl.closed.Load() {
		return 0, fmt.Errorf("command log is closed")
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	// Create entry
	seq := atomic.AddUint64(&cl.currentSeq, 1)
	entry := &CommandEntry{
		Sequence:  seq,
		Timestamp: time.Now().UnixNano(),
		CmdType:   cmdType,
		KeyLen:    uint32(len(key)),
		ValueLen:  uint32(len(value)),
		Key:       key,
		Value:     value,
	}

	// Encode and write
	data := entry.Encode()

	// Check if need to rotate log file
	if cl.currentSize+int64(len(data)) > LogFileMaxSize {
		if err := cl.rotateLogFile(); err != nil {
			return 0, fmt.Errorf("failed to rotate log: %w", err)
		}
	}

	// Write to buffer
	offset := cl.currentSize
	n, err := cl.currentWriter.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}
	cl.currentSize += int64(n)

	// Update index
	cl.index[seq] = logPosition{
		fileNum: cl.getFileNum(),
		offset:  offset,
	}

	// Signal sync if buffer is getting full
	if cl.currentWriter.Buffered() > LogBufferSize/2 {
		select {
		case cl.syncCh <- struct{}{}:
		default:
		}
	}

	return seq, nil
}

// AppendPut appends a put command
func (cl *CommandLog) AppendPut(key, value []byte) (uint64, error) {
	return cl.Append(CmdTypePut, key, value)
}

// AppendDelete appends a delete command
func (cl *CommandLog) AppendDelete(key []byte) (uint64, error) {
	return cl.Append(CmdTypeDelete, key, nil)
}

// AppendFieldBatch appends a field-level batch update command
// The batchData should be encoded using FieldBatch.Encode()
func (cl *CommandLog) AppendFieldBatch(batchData []byte) (uint64, error) {
	// For field batch, key is empty since pk is in batchData
	return cl.Append(CmdTypeFieldBatch, nil, batchData)
}

// GetCurrentSequence returns the current sequence number
func (cl *CommandLog) GetCurrentSequence() uint64 {
	return atomic.LoadUint64(&cl.currentSeq)
}

// GetSyncedSequence returns the last synced sequence number
func (cl *CommandLog) GetSyncedSequence() uint64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.lastSyncSeq
}

// ReadFrom reads entries starting from a sequence number
func (cl *CommandLog) ReadFrom(fromSeq uint64, limit int) ([]*CommandEntry, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	entries := make([]*CommandEntry, 0, limit)

	// Find all log files
	files, err := filepath.Glob(filepath.Join(cl.logDir, "*"+LogFileSuffix))
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		// Read entries from file
		for len(entries) < limit {
			entry, err := DecodeCommandEntry(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				cl.logger.Warn("Failed to decode entry", zap.Error(err))
				break
			}

			if entry.Sequence >= fromSeq {
				entries = append(entries, entry)
			}
		}

		f.Close()

		if len(entries) >= limit {
			break
		}
	}

	return entries, nil
}

// Sync flushes the buffer to disk
func (cl *CommandLog) Sync() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.currentWriter == nil {
		return nil
	}

	if err := cl.currentWriter.Flush(); err != nil {
		return err
	}

	if err := cl.currentFile.Sync(); err != nil {
		return err
	}

	cl.lastSyncSeq = cl.currentSeq
	return nil
}

// Close closes the command log
func (cl *CommandLog) Close() error {
	if !cl.closed.CompareAndSwap(false, true) {
		return nil
	}

	cl.syncTicker.Stop()

	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.currentWriter != nil {
		cl.currentWriter.Flush()
	}
	if cl.currentFile != nil {
		cl.currentFile.Sync()
		cl.currentFile.Close()
	}

	cl.logger.Info("Command log closed",
		zap.Uint32("partition_id", cl.partitionID),
		zap.Uint64("last_seq", cl.currentSeq),
	)

	return nil
}

// Truncate removes entries before the given sequence (for compaction)
func (cl *CommandLog) Truncate(beforeSeq uint64) error {
	// TODO: Implement log compaction
	// This would delete old log files that are fully replicated
	return nil
}

// recover loads existing log state
func (cl *CommandLog) recover() error {
	files, err := filepath.Glob(filepath.Join(cl.logDir, "*"+LogFileSuffix))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	// Find the last entry to get current sequence
	var maxSeq uint64
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		for {
			entry, err := DecodeCommandEntry(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				cl.logger.Warn("Recovery: corrupted entry", zap.String("file", file), zap.Error(err))
				break
			}

			if entry.Sequence > maxSeq {
				maxSeq = entry.Sequence
			}
		}

		f.Close()
	}

	cl.currentSeq = maxSeq
	cl.lastSyncSeq = maxSeq

	cl.logger.Info("Command log recovered",
		zap.Uint32("partition_id", cl.partitionID),
		zap.Int("file_count", len(files)),
		zap.Uint64("max_seq", maxSeq),
	)

	return nil
}

// openNewLogFile opens a new log file for writing
func (cl *CommandLog) openNewLogFile() error {
	filename := filepath.Join(cl.logDir, fmt.Sprintf("%020d%s", time.Now().UnixNano(), LogFileSuffix))

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	cl.currentFile = f
	cl.currentWriter = bufio.NewWriterSize(f, LogBufferSize)
	cl.currentSize = 0

	return nil
}

// rotateLogFile closes current file and opens a new one
func (cl *CommandLog) rotateLogFile() error {
	// Flush and close current
	if cl.currentWriter != nil {
		if err := cl.currentWriter.Flush(); err != nil {
			return err
		}
	}
	if cl.currentFile != nil {
		if err := cl.currentFile.Sync(); err != nil {
			return err
		}
		cl.currentFile.Close()
	}

	// Open new file
	return cl.openNewLogFile()
}

// syncLoop periodically syncs the log to disk
func (cl *CommandLog) syncLoop() {
	for {
		select {
		case <-cl.syncTicker.C:
			cl.Sync()
		case <-cl.syncCh:
			cl.Sync()
		}

		if cl.closed.Load() {
			return
		}
	}
}

// getFileNum returns current file number (for indexing)
func (cl *CommandLog) getFileNum() int64 {
	if cl.currentFile == nil {
		return 0
	}
	info, err := cl.currentFile.Stat()
	if err != nil {
		return 0
	}
	return info.ModTime().UnixNano()
}

// ReplayTo replays all commands from the log to a RocksDB instance
func (cl *CommandLog) ReplayTo(db *RocksDB, fromSeq uint64) (uint64, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	files, err := filepath.Glob(filepath.Join(cl.logDir, "*"+LogFileSuffix))
	if err != nil {
		return 0, err
	}

	var lastSeq uint64
	var replayed int64

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		for {
			entry, err := DecodeCommandEntry(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				cl.logger.Warn("Replay: corrupted entry", zap.Error(err))
				break
			}

			if entry.Sequence <= fromSeq {
				continue
			}

			// Apply command
			switch entry.CmdType {
			case CmdTypePut:
				if err := db.Put(entry.Key, entry.Value); err != nil {
					f.Close()
					return lastSeq, fmt.Errorf("replay put failed at seq %d: %w", entry.Sequence, err)
				}
			case CmdTypeDelete:
				if err := db.Delete(entry.Key); err != nil {
					f.Close()
					return lastSeq, fmt.Errorf("replay delete failed at seq %d: %w", entry.Sequence, err)
				}
			case CmdTypeFieldBatch:
				// Apply field batch: decode and apply each field update
				fb, err := DecodeFieldBatch(entry.Value)
				if err != nil {
					f.Close()
					return lastSeq, fmt.Errorf("replay field batch decode failed at seq %d: %w", entry.Sequence, err)
				}
				fs := NewFieldStorage(db)
				if err := fs.ApplyFieldBatch(fb); err != nil {
					f.Close()
					return lastSeq, fmt.Errorf("replay field batch apply failed at seq %d: %w", entry.Sequence, err)
				}
			}

			lastSeq = entry.Sequence
			replayed++
		}

		f.Close()
	}

	cl.logger.Info("Command log replay completed",
		zap.Uint32("partition_id", cl.partitionID),
		zap.Uint64("from_seq", fromSeq),
		zap.Uint64("last_seq", lastSeq),
		zap.Int64("entries_replayed", replayed),
	)

	return lastSeq, nil
}
