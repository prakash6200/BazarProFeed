package models

import (
	"encoding/json"
	"strings"
	"time"

	"gorm.io/gorm"
)

type GlobalTickEvent struct {
	ID        string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Exchange  string    `gorm:"type:text;index" json:"exchange"`
	Symbol    string    `gorm:"type:text;index" json:"symbol"`
	LTP       float64   `json:"ltp"`
	TickTime  time.Time `gorm:"index" json:"tick_time"`
	Payload   []byte    `gorm:"type:jsonb;not null" json:"payload"`
	CreatedAt time.Time `gorm:"not null;index" json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type GlobalCandleRow struct {
	Exchange      string    `json:"exchange"`
	Symbol        string    `json:"symbol"`
	IntervalStart time.Time `json:"interval_start"`
	Open          float64   `json:"open"`
	High          float64   `json:"high"`
	Low           float64   `json:"low"`
	Close         float64   `json:"close"`
	TickCount     int64     `json:"tick_count"`
}

type GlobalCandleTicksRow struct {
	Exchange      string            `json:"exchange"`
	Symbol        string            `json:"symbol"`
	IntervalStart time.Time         `json:"interval_start"`
	TickCount     int64             `json:"tick_count"`
	Ticks         []json.RawMessage `json:"ticks"`
}

type GlobalTickQueryFilters struct {
	Exchange      string
	Symbol        string
	IntervalStart *time.Time
	IntervalEnd   *time.Time
}

func CreateGlobalTickEvent(db *gorm.DB, exchange, symbol string, ltp float64, tickTime time.Time, payload []byte) error {
	if db == nil || len(payload) == 0 {
		return nil
	}
	event := GlobalTickEvent{
		Exchange: strings.ToUpper(strings.TrimSpace(exchange)),
		Symbol:   strings.ToUpper(strings.TrimSpace(symbol)),
		LTP:      ltp,
		TickTime: tickTime.UTC(),
		Payload:  payload,
	}
	return db.Create(&event).Error
}

func CreateGlobalTickEventsBatch(db *gorm.DB, events []GlobalTickEvent, batchSize int) error {
	if db == nil || len(events) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = 200
	}

	filtered := make([]GlobalTickEvent, 0, len(events))
	for _, event := range events {
		if len(event.Payload) == 0 {
			continue
		}
		event.Exchange = strings.ToUpper(strings.TrimSpace(event.Exchange))
		event.Symbol = strings.ToUpper(strings.TrimSpace(event.Symbol))
		event.TickTime = event.TickTime.UTC()
		filtered = append(filtered, event)
	}

	if len(filtered) == 0 {
		return nil
	}

	return db.CreateInBatches(filtered, batchSize).Error
}

func ListGlobalTickEventsLast24h(db *gorm.DB, page, sizePerPage int, filters GlobalTickQueryFilters) ([]json.RawMessage, int64, error) {
	if page < 1 {
		page = 1
	}
	if sizePerPage < 1 {
		sizePerPage = 20
	}

	q := db.Model(&GlobalTickEvent{}).Where("created_at >= NOW() - INTERVAL '24 hours'")
	if strings.TrimSpace(filters.Exchange) != "" {
		q = q.Where("exchange = ?", strings.ToUpper(strings.TrimSpace(filters.Exchange)))
	}
	if strings.TrimSpace(filters.Symbol) != "" {
		q = q.Where("symbol = ?", strings.ToUpper(strings.TrimSpace(filters.Symbol)))
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var rows []GlobalTickEvent
	offset := (page - 1) * sizePerPage
	if err := q.Order("created_at DESC").Offset(offset).Limit(sizePerPage).Find(&rows).Error; err != nil {
		return nil, 0, err
	}

	result := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		result = append(result, json.RawMessage(row.Payload))
	}
	return result, total, nil
}

func ListGlobalCandlesLast24h(db *gorm.DB, intervalMinutes, page, sizePerPage int, filters GlobalTickQueryFilters) ([]GlobalCandleRow, int64, error) {
	if page < 1 {
		page = 1
	}
	if sizePerPage < 1 {
		sizePerPage = 20
	}
	if intervalMinutes <= 0 {
		intervalMinutes = 1
	}

	whereSQL := "WHERE created_at >= NOW() - INTERVAL '24 hours'"
	args := []interface{}{}
	if strings.TrimSpace(filters.Exchange) != "" {
		whereSQL += " AND exchange = ?"
		args = append(args, strings.ToUpper(strings.TrimSpace(filters.Exchange)))
	}
	if strings.TrimSpace(filters.Symbol) != "" {
		whereSQL += " AND symbol = ?"
		args = append(args, strings.ToUpper(strings.TrimSpace(filters.Symbol)))
	}
	if filters.IntervalStart != nil {
		whereSQL += " AND (CASE WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at ELSE tick_time END) >= ?"
		args = append(args, filters.IntervalStart.UTC())
	}
	if filters.IntervalEnd != nil {
		whereSQL += " AND (CASE WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at ELSE tick_time END) < ?"
		args = append(args, filters.IntervalEnd.UTC())
	}

	bucketExpr := "date_trunc('hour', tick_ts) + floor(date_part('minute', tick_ts) / ?) * make_interval(mins => ?)"

	countSQL := `
	WITH filtered AS (
		SELECT exchange, symbol, ltp,
			CASE
				WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at
				ELSE tick_time
			END AS tick_ts
		FROM global_tick_events
		` + whereSQL + `
	), grouped AS (
		SELECT exchange, symbol, ` + bucketExpr + ` AS interval_start
		FROM filtered
		GROUP BY exchange, symbol, interval_start
	)
	SELECT COUNT(*) FROM grouped`

	countArgs := append([]interface{}{}, args...)
	countArgs = append(countArgs, intervalMinutes, intervalMinutes)

	var total int64
	if err := db.Raw(countSQL, countArgs...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * sizePerPage

	dataSQL := `
	WITH filtered AS (
		SELECT exchange, symbol, ltp,
			CASE
				WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at
				ELSE tick_time
			END AS tick_ts
		FROM global_tick_events
		` + whereSQL + `
	), bucketed AS (
		SELECT exchange, symbol, ltp, tick_ts,
			` + bucketExpr + ` AS interval_start
		FROM filtered
	), aggregated AS (
		SELECT
			exchange,
			symbol,
			interval_start,
			(array_agg(ltp ORDER BY tick_ts ASC))[1] AS open,
			MAX(ltp) AS high,
			MIN(ltp) AS low,
			(array_agg(ltp ORDER BY tick_ts DESC))[1] AS close,
			COUNT(*) AS tick_count
		FROM bucketed
		GROUP BY exchange, symbol, interval_start
	)
	SELECT exchange, symbol, interval_start, open, high, low, close, tick_count
	FROM aggregated
	ORDER BY interval_start DESC
	OFFSET ? LIMIT ?`

	dataArgs := append([]interface{}{}, args...)
	dataArgs = append(dataArgs, intervalMinutes, intervalMinutes, offset, sizePerPage)

	var rows []GlobalCandleRow
	if err := db.Raw(dataSQL, dataArgs...).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}

	return rows, total, nil
}

func ListGlobalCandleTicksLast24h(db *gorm.DB, intervalMinutes, page, sizePerPage int, filters GlobalTickQueryFilters) ([]GlobalCandleTicksRow, int64, error) {
	if page < 1 {
		page = 1
	}
	if sizePerPage < 1 {
		sizePerPage = 20
	}
	if intervalMinutes <= 0 {
		intervalMinutes = 1
	}

	whereSQL := "WHERE created_at >= NOW() - INTERVAL '24 hours'"
	args := []interface{}{}
	if strings.TrimSpace(filters.Exchange) != "" {
		whereSQL += " AND exchange = ?"
		args = append(args, strings.ToUpper(strings.TrimSpace(filters.Exchange)))
	}
	if strings.TrimSpace(filters.Symbol) != "" {
		whereSQL += " AND symbol = ?"
		args = append(args, strings.ToUpper(strings.TrimSpace(filters.Symbol)))
	}
	if filters.IntervalStart != nil {
		whereSQL += " AND (CASE WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at ELSE tick_time END) >= ?"
		args = append(args, filters.IntervalStart.UTC())
	}
	if filters.IntervalEnd != nil {
		whereSQL += " AND (CASE WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at ELSE tick_time END) < ?"
		args = append(args, filters.IntervalEnd.UTC())
	}

	if filters.IntervalStart != nil && filters.IntervalEnd != nil {
		type directRow struct {
			Exchange  string `gorm:"column:exchange"`
			Symbol    string `gorm:"column:symbol"`
			TickCount int64  `gorm:"column:tick_count"`
			TicksRaw  []byte `gorm:"column:ticks"`
		}

		directSQL := `
		WITH filtered AS (
			SELECT exchange, symbol, payload,
				CASE
					WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at
					ELSE tick_time
				END AS tick_ts
			FROM global_tick_events
			` + whereSQL + `
		)
		SELECT
			MAX(exchange) AS exchange,
			MAX(symbol) AS symbol,
			COUNT(*) AS tick_count,
			COALESCE(jsonb_agg(payload ORDER BY tick_ts ASC), '[]'::jsonb) AS ticks
		FROM filtered`

		var row directRow
		if err := db.Raw(directSQL, args...).Scan(&row).Error; err != nil {
			return nil, 0, err
		}
		if row.TickCount == 0 {
			return nil, 0, nil
		}

		var ticks []json.RawMessage
		if len(row.TicksRaw) > 0 {
			_ = json.Unmarshal(row.TicksRaw, &ticks)
		}

		return []GlobalCandleTicksRow{{
			Exchange:      row.Exchange,
			Symbol:        row.Symbol,
			IntervalStart: filters.IntervalStart.UTC(),
			TickCount:     row.TickCount,
			Ticks:         ticks,
		}}, 1, nil
	}

	bucketExpr := "date_trunc('hour', tick_ts) + floor(date_part('minute', tick_ts) / ?) * make_interval(mins => ?)"

	countSQL := `
	WITH filtered AS (
		SELECT exchange, symbol,
			CASE
				WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at
				ELSE tick_time
			END AS tick_ts
		FROM global_tick_events
		` + whereSQL + `
	), grouped AS (
		SELECT exchange, symbol, ` + bucketExpr + ` AS interval_start
		FROM filtered
		GROUP BY exchange, symbol, interval_start
	)
	SELECT COUNT(*) FROM grouped`

	countArgs := append([]interface{}{}, args...)
	countArgs = append(countArgs, intervalMinutes, intervalMinutes)

	var total int64
	if err := db.Raw(countSQL, countArgs...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * sizePerPage

	dataSQL := `
	WITH filtered AS (
		SELECT exchange, symbol, payload,
			CASE
				WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at
				ELSE tick_time
			END AS tick_ts
		FROM global_tick_events
		` + whereSQL + `
	), bucketed AS (
		SELECT exchange, symbol, payload, tick_ts,
			` + bucketExpr + ` AS interval_start
		FROM filtered
	), grouped AS (
		SELECT
			exchange,
			symbol,
			interval_start,
			COUNT(*) AS tick_count,
			jsonb_agg(payload ORDER BY tick_ts ASC) AS ticks
		FROM bucketed
		GROUP BY exchange, symbol, interval_start
	)
	SELECT exchange, symbol, interval_start, tick_count, ticks
	FROM grouped
	ORDER BY interval_start DESC
	OFFSET ? LIMIT ?`

	dataArgs := append([]interface{}{}, args...)
	dataArgs = append(dataArgs, intervalMinutes, intervalMinutes, offset, sizePerPage)

	type row struct {
		Exchange      string    `gorm:"column:exchange"`
		Symbol        string    `gorm:"column:symbol"`
		IntervalStart time.Time `gorm:"column:interval_start"`
		TickCount     int64     `gorm:"column:tick_count"`
		TicksRaw      []byte    `gorm:"column:ticks"`
	}

	var rows []row
	if err := db.Raw(dataSQL, dataArgs...).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}

	result := make([]GlobalCandleTicksRow, 0, len(rows))
	for _, r := range rows {
		var ticks []json.RawMessage
		if len(r.TicksRaw) > 0 {
			_ = json.Unmarshal(r.TicksRaw, &ticks)
		}
		result = append(result, GlobalCandleTicksRow{
			Exchange:      r.Exchange,
			Symbol:        r.Symbol,
			IntervalStart: r.IntervalStart,
			TickCount:     r.TickCount,
			Ticks:         ticks,
		})
	}

	return result, total, nil
}
