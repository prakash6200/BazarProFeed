package services

import (
	"context"
	"fmt"
	"log"
	"time"

	"gorm.io/gorm"
)

var feedEventRetentionTables = []string{"zerodha_tick_events", "global_tick_events"}

// RunFeedRetentionOnce deletes feed rows older than olderThan from all
// feed event tables. Deletion runs in small batches to avoid long table locks.
func RunFeedRetentionOnce(ctx context.Context, db *gorm.DB, olderThan time.Duration, batchSize int) (int64, error) {
	if db == nil {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if olderThan <= 0 {
		olderThan = 7 * 24 * time.Hour
	}
	if batchSize <= 0 {
		batchSize = 5000
	}

	cutoff := time.Now().UTC().Add(-olderThan)
	totalDeleted := int64(0)

	for _, table := range feedEventRetentionTables {
		deleted, err := purgeFeedTableInBatches(ctx, db, table, cutoff, batchSize)
		if err != nil {
			return totalDeleted, err
		}
		totalDeleted += deleted
	}

	return totalDeleted, nil
}

// StartFeedRetentionCron schedules feed retention to run once per day at
// 02:30 IST (21:00 UTC). The server is assumed to run on UTC.
func StartFeedRetentionCron(ctx context.Context, db *gorm.DB, olderThan time.Duration, batchSize int) {
	if db == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// 02:30 IST = UTC+5:30 → 21:00 UTC the previous calendar day.
	// We schedule relative to UTC so the server clock (UTC) is always authoritative.
	const cronHourUTC = 21
	const cronMinUTC = 0

	for {
		now := time.Now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day(), cronHourUTC, cronMinUTC, 0, 0, time.UTC)
		if !now.Before(next) {
			next = next.Add(24 * time.Hour)
		}
		wait := time.Until(next)
		log.Printf("feed retention: next run scheduled at %s UTC (02:30 IST) in %s",
			next.Format("2006-01-02 15:04"), wait.Round(time.Second))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		deleted, err := RunFeedRetentionOnce(runCtx, db, olderThan, batchSize)
		cancel()
		if err != nil {
			log.Printf("feed retention: cleanup failed: %v", err)
			continue
		}
		if deleted > 0 {
			log.Printf("feed retention: deleted %d rows older than %s", deleted, olderThan)
		} else {
			log.Printf("feed retention: no rows to delete")
		}
	}
}

func purgeFeedTableInBatches(ctx context.Context, db *gorm.DB, table string, cutoff time.Time, batchSize int) (int64, error) {
	totalDeleted := int64(0)
	// PostgreSQL does not allow a table alias on the target of DELETE.
	// The correct form is: DELETE FROM tbl WHERE id IN (SELECT id FROM …).
	query := fmt.Sprintf(`
DELETE FROM %s
WHERE id IN (
	SELECT id
	FROM %s
	WHERE created_at < ?
	ORDER BY created_at ASC
	LIMIT ?
)`, table, table)

	for {
		result := db.WithContext(ctx).Exec(query, cutoff, batchSize)
		if result.Error != nil {
			return totalDeleted, result.Error
		}
		if result.RowsAffected == 0 {
			break
		}
		totalDeleted += result.RowsAffected
	}

	return totalDeleted, nil
}
