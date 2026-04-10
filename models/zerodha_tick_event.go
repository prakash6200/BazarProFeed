package models

import (
	"encoding/json"
	"strings"
	"time"

	"gorm.io/gorm"
)

type ZerodhaTickEvent struct {
	ID        string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Exchange  string    `gorm:"type:text;index" json:"exchange"`
	Symbol    string    `gorm:"type:text;index" json:"symbol"`
	LTP       float64   `json:"ltp"`
	TickTime  time.Time `gorm:"index" json:"tick_time"`
	Payload   []byte    `gorm:"type:jsonb;not null" json:"payload"`
	CreatedAt time.Time `gorm:"index" json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ZerodhaCandleRow struct {
	Exchange      string    `json:"exchange"`
	Symbol        string    `json:"symbol"`
	IntervalStart time.Time `json:"interval_start"`
	Open          float64   `json:"open"`
	High          float64   `json:"high"`
	Low           float64   `json:"low"`
	Close         float64   `json:"close"`
	TickCount     int64     `json:"tick_count"`
}

type ZerodhaCandleTicksRow struct {
	Exchange      string            `json:"exchange"`
	Symbol        string            `json:"symbol"`
	IntervalStart time.Time         `json:"interval_start"`
	TickCount     int64             `json:"tick_count"`
	Ticks         []json.RawMessage `json:"ticks"`
}

type ZerodhaTickQueryFilters struct {
	Exchange string
	Symbol   string
}

func CreateZerodhaTickEvent(db *gorm.DB, exchange, symbol string, ltp float64, tickTime time.Time, payload []byte) error {
	if db == nil || len(payload) == 0 {
		return nil
	}
	event := ZerodhaTickEvent{
		Exchange: strings.ToUpper(strings.TrimSpace(exchange)),
		Symbol:   strings.ToUpper(strings.TrimSpace(symbol)),
		LTP:      ltp,
		TickTime: tickTime.UTC(),
		Payload:  payload,
	}
	return db.Create(&event).Error
}

func ListZerodhaCandlesLast24h(db *gorm.DB, intervalMinutes, page, sizePerPage int, filters ZerodhaTickQueryFilters) ([]ZerodhaCandleRow, int64, error) {
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

	bucketExpr := "date_trunc('hour', tick_ts) + floor(date_part('minute', tick_ts) / ?) * make_interval(mins => ?)"

	countSQL := `
	WITH filtered AS (
		SELECT exchange, symbol, ltp,
			CASE
				WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at
				ELSE tick_time
			END AS tick_ts
		FROM zerodha_tick_events
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
		FROM zerodha_tick_events
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

	var rows []ZerodhaCandleRow
	if err := db.Raw(dataSQL, dataArgs...).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}

	return rows, total, nil
}

func ListZerodhaCandleTicksLast24h(db *gorm.DB, intervalMinutes, page, sizePerPage int, filters ZerodhaTickQueryFilters) ([]ZerodhaCandleTicksRow, int64, error) {
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

	bucketExpr := "date_trunc('hour', tick_ts) + floor(date_part('minute', tick_ts) / ?) * make_interval(mins => ?)"

	countSQL := `
	WITH filtered AS (
		SELECT exchange, symbol,
			CASE
				WHEN tick_time IS NULL OR tick_time <= '1970-01-01'::timestamp THEN created_at
				ELSE tick_time
			END AS tick_ts
		FROM zerodha_tick_events
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
		FROM zerodha_tick_events
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

	result := make([]ZerodhaCandleTicksRow, 0, len(rows))
	for _, r := range rows {
		var ticks []json.RawMessage
		if len(r.TicksRaw) > 0 {
			_ = json.Unmarshal(r.TicksRaw, &ticks)
		}
		result = append(result, ZerodhaCandleTicksRow{
			Exchange:      r.Exchange,
			Symbol:        r.Symbol,
			IntervalStart: r.IntervalStart,
			TickCount:     r.TickCount,
			Ticks:         ticks,
		})
	}

	return result, total, nil
}
