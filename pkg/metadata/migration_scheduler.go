package metadata

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/winewei/rockskv/pkg/common"
	pb "github.com/winewei/rockskv/pkg/proto"
)

const (
	// DefaultMaxConcurrent is the default max concurrent migrations
	DefaultMaxConcurrent = 4

	// DefaultBandwidthLimit is the default bandwidth limit (50MB/s per migration)
	DefaultBandwidthLimit = 50 * 1024 * 1024

	// DefaultMigrationTimeout is the default timeout for a single migration
	DefaultMigrationTimeout = 10 * time.Minute

	// MigrationCheckInterval is how often to check migration progress
	MigrationCheckInterval = 1 * time.Second
)

// MigrationTask represents a single partition migration task
type MigrationTask struct {
	PartitionID uint32
	Source      string
	Target      string
	Priority    pb.MigrationPriority
	IsPrimary   bool

	// Progress tracking
	State         pb.MigrationState
	BytesCopied   int64
	BytesTotal    int64
	StartedAt     time.Time
	UpdatedAt     time.Time
	CompletedAt   time.Time
	ErrorMessage  string

	// Control
	cancel context.CancelFunc
}

// MigrationScheduler manages partition migrations with concurrency and bandwidth control
type MigrationScheduler struct {
	router *Router
	store  Store
	logger *zap.Logger

	// Configuration
	maxConcurrent  int32
	bandwidthLimit int64 // bytes per second per migration

	// Task management
	pendingTasks  []*MigrationTask
	activeTasks   map[uint32]*MigrationTask // partitionID -> task
	completedTasks []*MigrationTask
	tasksMu       sync.RWMutex

	// Statistics
	totalMigrated    int64
	totalFailed      int64
	totalBytesTransferred int64

	// Control
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	isRunning atomic.Bool
}

// NewMigrationScheduler creates a new migration scheduler
func NewMigrationScheduler(router *Router, store Store) *MigrationScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &MigrationScheduler{
		router:         router,
		store:          store,
		logger:         common.NewLogger("migration-scheduler"),
		maxConcurrent:  DefaultMaxConcurrent,
		bandwidthLimit: DefaultBandwidthLimit,
		activeTasks:    make(map[uint32]*MigrationTask),
		pendingTasks:   make([]*MigrationTask, 0),
		completedTasks: make([]*MigrationTask, 0),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// SetConfig updates scheduler configuration
func (ms *MigrationScheduler) SetConfig(maxConcurrent int32, bandwidthLimit int64) {
	if maxConcurrent > 0 {
		atomic.StoreInt32(&ms.maxConcurrent, maxConcurrent)
	}
	if bandwidthLimit > 0 {
		atomic.StoreInt64(&ms.bandwidthLimit, bandwidthLimit)
	}
	ms.logger.Info("Migration scheduler config updated",
		zap.Int32("max_concurrent", atomic.LoadInt32(&ms.maxConcurrent)),
		zap.Int64("bandwidth_limit", atomic.LoadInt64(&ms.bandwidthLimit)),
	)
}

// Start starts the migration scheduler
func (ms *MigrationScheduler) Start() {
	if ms.isRunning.Load() {
		return
	}
	ms.isRunning.Store(true)

	ms.wg.Add(1)
	go ms.schedulerLoop()

	ms.logger.Info("Migration scheduler started",
		zap.Int32("max_concurrent", ms.maxConcurrent),
		zap.Int64("bandwidth_limit", ms.bandwidthLimit),
	)
}

// Stop stops the migration scheduler
func (ms *MigrationScheduler) Stop() {
	if !ms.isRunning.Load() {
		return
	}
	ms.isRunning.Store(false)
	ms.cancel()
	ms.wg.Wait()
	ms.logger.Info("Migration scheduler stopped")
}

// AddTask adds a migration task to the scheduler
func (ms *MigrationScheduler) AddTask(task *MigrationTask) error {
	ms.tasksMu.Lock()
	defer ms.tasksMu.Unlock()

	// Check if already exists
	if _, exists := ms.activeTasks[task.PartitionID]; exists {
		return fmt.Errorf("partition %d already has active migration", task.PartitionID)
	}

	for _, t := range ms.pendingTasks {
		if t.PartitionID == task.PartitionID {
			return fmt.Errorf("partition %d already in pending queue", task.PartitionID)
		}
	}

	task.State = pb.MigrationState_MIGRATION_PREPARING
	task.StartedAt = time.Time{} // Will be set when actually started
	task.UpdatedAt = time.Now()

	// Insert by priority (higher priority first)
	inserted := false
	for i, t := range ms.pendingTasks {
		if task.Priority > t.Priority {
			ms.pendingTasks = append(ms.pendingTasks[:i], append([]*MigrationTask{task}, ms.pendingTasks[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		ms.pendingTasks = append(ms.pendingTasks, task)
	}

	ms.logger.Info("Migration task added",
		zap.Uint32("partition_id", task.PartitionID),
		zap.String("source", task.Source),
		zap.String("target", task.Target),
		zap.Int32("priority", int32(task.Priority)),
	)

	return nil
}

// AddTasks adds multiple migration tasks (batch operation)
func (ms *MigrationScheduler) AddTasks(tasks []*MigrationTask) error {
	for _, task := range tasks {
		if err := ms.AddTask(task); err != nil {
			return err
		}
	}
	return nil
}

// CancelTask cancels a specific migration task
func (ms *MigrationScheduler) CancelTask(partitionID uint32) error {
	ms.tasksMu.Lock()
	defer ms.tasksMu.Unlock()

	// Check active tasks
	if task, exists := ms.activeTasks[partitionID]; exists {
		if task.cancel != nil {
			task.cancel()
		}
		task.State = pb.MigrationState_MIGRATION_FAILED
		task.ErrorMessage = "cancelled by user"
		task.CompletedAt = time.Now()
		ms.completedTasks = append(ms.completedTasks, task)
		delete(ms.activeTasks, partitionID)
		atomic.AddInt64(&ms.totalFailed, 1)
		return nil
	}

	// Check pending tasks
	for i, task := range ms.pendingTasks {
		if task.PartitionID == partitionID {
			ms.pendingTasks = append(ms.pendingTasks[:i], ms.pendingTasks[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("partition %d not found in migration queue", partitionID)
}

// CancelAllTasks cancels all pending and active migrations
func (ms *MigrationScheduler) CancelAllTasks() {
	ms.tasksMu.Lock()
	defer ms.tasksMu.Unlock()

	// Cancel active tasks
	for partitionID, task := range ms.activeTasks {
		if task.cancel != nil {
			task.cancel()
		}
		task.State = pb.MigrationState_MIGRATION_FAILED
		task.ErrorMessage = "cancelled (all tasks)"
		task.CompletedAt = time.Now()
		ms.completedTasks = append(ms.completedTasks, task)
		delete(ms.activeTasks, partitionID)
		atomic.AddInt64(&ms.totalFailed, 1)
	}

	// Clear pending tasks
	ms.pendingTasks = ms.pendingTasks[:0]

	ms.logger.Info("All migration tasks cancelled")
}

// GetStatus returns the current migration status
func (ms *MigrationScheduler) GetStatus() *MigrationStatus {
	ms.tasksMu.RLock()
	defer ms.tasksMu.RUnlock()

	status := &MigrationStatus{
		IsRunning:      ms.isRunning.Load(),
		MaxConcurrent:  atomic.LoadInt32(&ms.maxConcurrent),
		BandwidthLimit: atomic.LoadInt64(&ms.bandwidthLimit),
		PendingCount:   len(ms.pendingTasks),
		ActiveCount:    len(ms.activeTasks),
		TotalMigrated:  atomic.LoadInt64(&ms.totalMigrated),
		TotalFailed:    atomic.LoadInt64(&ms.totalFailed),
		TotalBytes:     atomic.LoadInt64(&ms.totalBytesTransferred),
		ActiveTasks:    make([]*MigrationTask, 0, len(ms.activeTasks)),
	}

	for _, task := range ms.activeTasks {
		status.ActiveTasks = append(status.ActiveTasks, task)
	}

	return status
}

// MigrationStatus contains scheduler status information
type MigrationStatus struct {
	IsRunning      bool
	MaxConcurrent  int32
	BandwidthLimit int64
	PendingCount   int
	ActiveCount    int
	TotalMigrated  int64
	TotalFailed    int64
	TotalBytes     int64
	ActiveTasks    []*MigrationTask
}

// schedulerLoop is the main scheduler loop
func (ms *MigrationScheduler) schedulerLoop() {
	defer ms.wg.Done()

	ticker := time.NewTicker(MigrationCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ms.ctx.Done():
			return
		case <-ticker.C:
			ms.processQueue()
		}
	}
}

// processQueue processes the migration queue
func (ms *MigrationScheduler) processQueue() {
	ms.tasksMu.Lock()

	// Check how many slots are available
	currentActive := int32(len(ms.activeTasks))
	maxConcurrent := atomic.LoadInt32(&ms.maxConcurrent)
	availableSlots := maxConcurrent - currentActive

	if availableSlots <= 0 || len(ms.pendingTasks) == 0 {
		ms.tasksMu.Unlock()
		return
	}

	// Start new tasks
	tasksToStart := make([]*MigrationTask, 0, availableSlots)
	for i := 0; i < int(availableSlots) && i < len(ms.pendingTasks); i++ {
		task := ms.pendingTasks[i]
		tasksToStart = append(tasksToStart, task)
	}

	// Remove from pending and add to active
	ms.pendingTasks = ms.pendingTasks[len(tasksToStart):]
	for _, task := range tasksToStart {
		_, cancel := context.WithTimeout(ms.ctx, DefaultMigrationTimeout)
		task.cancel = cancel
		task.StartedAt = time.Now()
		task.State = pb.MigrationState_MIGRATION_COPYING
		ms.activeTasks[task.PartitionID] = task
	}

	ms.tasksMu.Unlock()

	// Start migrations (outside lock)
	for _, task := range tasksToStart {
		ms.wg.Add(1)
		go ms.executeMigration(task)
	}
}

// executeMigration executes a single migration task
func (ms *MigrationScheduler) executeMigration(task *MigrationTask) {
	defer ms.wg.Done()

	ctx := ms.ctx
	if task.cancel != nil {
		ctx, _ = context.WithTimeout(ms.ctx, DefaultMigrationTimeout)
	}

	ms.logger.Info("Starting migration",
		zap.Uint32("partition_id", task.PartitionID),
		zap.String("source", task.Source),
		zap.String("target", task.Target),
	)

	// Update migration state in etcd
	migrationInfo := &MigrationInfo{
		PartitionID:   task.PartitionID,
		Target:        task.Target,
		State:         pb.MigrationState_MIGRATION_COPYING,
		IsPrimaryMove: task.IsPrimary,
		StartedAt:     task.StartedAt,
		UpdatedAt:     time.Now(),
	}
	if err := ms.store.SetMigrationState(ctx, task.PartitionID, migrationInfo); err != nil {
		ms.completeMigration(task, fmt.Errorf("failed to set migration state: %w", err))
		return
	}

	// TODO: Actual data migration logic would go here
	// For now, we simulate the migration by updating the route table
	// In a real implementation, this would:
	// 1. Export SST from source
	// 2. Transfer with bandwidth limiting
	// 3. Ingest at target
	// 4. Catch up incremental changes
	// 5. Switch routing

	// Simulate migration delay for testing
	select {
	case <-ctx.Done():
		ms.completeMigration(task, ctx.Err())
		return
	case <-time.After(100 * time.Millisecond):
		// Migration "completed"
	}

	// Update route table to complete migration
	if err := ms.router.CompleteMigration(ctx, task.PartitionID, task.Target, task.IsPrimary); err != nil {
		ms.completeMigration(task, err)
		return
	}

	ms.completeMigration(task, nil)
}

// completeMigration marks a migration as complete
func (ms *MigrationScheduler) completeMigration(task *MigrationTask, err error) {
	ms.tasksMu.Lock()
	defer ms.tasksMu.Unlock()

	task.CompletedAt = time.Now()
	task.UpdatedAt = time.Now()

	if err != nil {
		task.State = pb.MigrationState_MIGRATION_FAILED
		task.ErrorMessage = err.Error()
		atomic.AddInt64(&ms.totalFailed, 1)
		ms.logger.Error("Migration failed",
			zap.Uint32("partition_id", task.PartitionID),
			zap.Error(err),
		)
	} else {
		task.State = pb.MigrationState_MIGRATION_COMPLETE
		atomic.AddInt64(&ms.totalMigrated, 1)
		atomic.AddInt64(&ms.totalBytesTransferred, task.BytesCopied)
		ms.logger.Info("Migration completed",
			zap.Uint32("partition_id", task.PartitionID),
			zap.Duration("duration", task.CompletedAt.Sub(task.StartedAt)),
		)
	}

	ms.completedTasks = append(ms.completedTasks, task)
	delete(ms.activeTasks, task.PartitionID)

	// Clean migration state in etcd
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ms.store.DeleteMigrationState(ctx, task.PartitionID)
}

// WaitForCompletion waits for all migrations to complete
func (ms *MigrationScheduler) WaitForCompletion(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ms.tasksMu.RLock()
			pending := len(ms.pendingTasks)
			active := len(ms.activeTasks)
			ms.tasksMu.RUnlock()

			if pending == 0 && active == 0 {
				return nil
			}
		}
	}
}

// GetPendingCount returns the number of pending migrations
func (ms *MigrationScheduler) GetPendingCount() int {
	ms.tasksMu.RLock()
	defer ms.tasksMu.RUnlock()
	return len(ms.pendingTasks)
}

// GetActiveCount returns the number of active migrations
func (ms *MigrationScheduler) GetActiveCount() int {
	ms.tasksMu.RLock()
	defer ms.tasksMu.RUnlock()
	return len(ms.activeTasks)
}
