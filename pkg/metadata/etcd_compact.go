package metadata

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// StartAutoCompact starts automatic compaction of etcd history
func (s *EtcdStore) StartAutoCompact(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.compactHistory(ctx); err != nil {
				s.logger.Warn("Auto compact failed", zap.Error(err))
			}
		}
	}
}

func (s *EtcdStore) compactHistory(ctx context.Context) error {
	// Get current revision
	resp, err := s.client.Status(ctx, s.client.Endpoints()[0])
	if err != nil {
		return err
	}

	currentRevision := resp.Header.Revision

	// Keep last 100 revisions, compact the rest
	if currentRevision > 100 {
		compactRevision := currentRevision - 100

		s.logger.Info("Compacting etcd history",
			zap.Int64("current_revision", currentRevision),
			zap.Int64("compact_to", compactRevision),
		)

		_, err = s.client.Compact(ctx, compactRevision)
		if err != nil {
			return err
		}

		// Also defragment to reclaim space
		// Note: In production, defrag should be done carefully as it locks the database
		_, err = s.client.Defragment(ctx, s.client.Endpoints()[0])
		if err != nil {
			s.logger.Warn("Defragmentation failed", zap.Error(err))
		} else {
			s.logger.Info("Defragmentation completed")
		}
	}

	return nil
}
