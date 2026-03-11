package models

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Instrument struct {
	ID              string     `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	InstrumentToken int64      `gorm:"uniqueIndex;not null" json:"instrument_token"`
	ExchangeToken   int64      `gorm:"not null" json:"exchange_token"`
	TradingSymbol   string     `gorm:"index;not null" json:"tradingsymbol"`
	Name            string     `json:"name"`
	LastPrice       float64    `json:"last_price"`
	Expiry          *time.Time `json:"expiry,omitempty"`
	Strike          float64    `json:"strike"`
	TickSize        float64    `json:"tick_size"`
	LotSize         int        `json:"lot_size"`
	InstrumentType  string     `json:"instrument_type"`
	Segment         string     `json:"segment"`
	Exchange        string     `json:"exchange"`
	Status          string     `gorm:"type:instrument_status;not null;default:'ACTIVE';index" json:"status"`
	IsDeleted       bool       `gorm:"not null;default:false;index" json:"is_deleted"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

const (
	InstrumentStatusActive   = "ACTIVE"
	InstrumentStatusInactive = "INACTIVE"
)

func NormalizeInstrumentStatus(status string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(status))
	if trimmed == InstrumentStatusInactive {
		return InstrumentStatusInactive
	}
	return InstrumentStatusActive
}

func (i *Instrument) IsActive() bool {
	return NormalizeInstrumentStatus(i.Status) == InstrumentStatusActive
}

type CSVImportResult struct {
	TotalRows int `json:"total_rows"`
	Imported  int `json:"imported"`
	Skipped   int `json:"skipped"`
}

func parseCSVInt64(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}
	return strconv.ParseInt(trimmed, 10, 64)
}

func parseCSVInt(value string) (int, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 32)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}

func parseCSVFloat(value string) (float64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}
	return strconv.ParseFloat(trimmed, 64)
}

func parseExpiry(value string) (*time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}
	parsed, err := time.Parse("02-01-06", trimmed)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func normalizeHeader(header []string) []string {
	normalized := make([]string, len(header))
	for i, item := range header {
		normalized[i] = strings.ToLower(strings.TrimSpace(item))
	}
	return normalized
}

func validateCSVHeader(header []string) bool {
	expected := []string{
		"instrument_token",
		"exchange_token",
		"tradingsymbol",
		"name",
		"last_price",
		"expiry",
		"strike",
		"tick_size",
		"lot_size",
		"instrument_type",
		"segment",
		"exchange",
	}

	if len(header) != len(expected) {
		return false
	}

	for i := range expected {
		if header[i] != expected[i] {
			return false
		}
	}

	return true
}

func parseInstrumentCSVRecord(record []string) (*Instrument, error) {
	if len(record) < 12 {
		return nil, errors.New("invalid csv row length")
	}

	instrumentToken, err := parseCSVInt64(record[0])
	if err != nil {
		return nil, err
	}

	exchangeToken, err := parseCSVInt64(record[1])
	if err != nil {
		return nil, err
	}

	lastPrice, err := parseCSVFloat(record[4])
	if err != nil {
		return nil, err
	}

	expiry, err := parseExpiry(record[5])
	if err != nil {
		return nil, err
	}

	strike, err := parseCSVFloat(record[6])
	if err != nil {
		return nil, err
	}

	tickSize, err := parseCSVFloat(record[7])
	if err != nil {
		return nil, err
	}

	lotSize, err := parseCSVInt(record[8])
	if err != nil {
		return nil, err
	}

	return &Instrument{
		InstrumentToken: instrumentToken,
		ExchangeToken:   exchangeToken,
		TradingSymbol:   strings.TrimSpace(record[2]),
		Name:            strings.TrimSpace(record[3]),
		LastPrice:       lastPrice,
		Expiry:          expiry,
		Strike:          strike,
		TickSize:        tickSize,
		LotSize:         lotSize,
		InstrumentType:  strings.TrimSpace(record[9]),
		Segment:         strings.TrimSpace(record[10]),
		Exchange:        strings.TrimSpace(record[11]),
		Status:          InstrumentStatusActive,
		IsDeleted:       false,
	}, nil
}

func ImportInstrumentsFromCSV(db *gorm.DB, csvPath string) (*CSVImportResult, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	normalized := normalizeHeader(header)
	if !validateCSVHeader(normalized) {
		return nil, fmt.Errorf("invalid csv header")
	}

	result := &CSVImportResult{}
	batch := make([]Instrument, 0, 1000)

	flush := func(rows []Instrument) error {
		if len(rows) == 0 {
			return nil
		}

		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "instrument_token"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"exchange_token",
				"trading_symbol",
				"name",
				"last_price",
				"expiry",
				"strike",
				"tick_size",
				"lot_size",
				"instrument_type",
				"segment",
				"exchange",
				"status",
				"is_deleted",
				"updated_at",
			}),
		}).Create(&rows).Error; err != nil {
			return err
		}

		result.Imported += len(rows)
		return nil
	}

	for {
		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			result.Skipped++
			continue
		}

		result.TotalRows++
		instrument, parseErr := parseInstrumentCSVRecord(record)
		if parseErr != nil {
			result.Skipped++
			continue
		}

		batch = append(batch, *instrument)
		if len(batch) >= 1000 {
			if err := flush(batch); err != nil {
				return nil, err
			}
			batch = batch[:0]
		}
	}

	if err := flush(batch); err != nil {
		return nil, err
	}

	return result, nil
}

func CreateInstrument(db *gorm.DB, instrument *Instrument) error {
	instrument.Status = NormalizeInstrumentStatus(instrument.Status)
	instrument.IsDeleted = false
	return db.Create(instrument).Error
}

func GetInstrumentByID(db *gorm.DB, id string) (*Instrument, error) {
	var instrument Instrument
	err := db.Where("id = ? AND is_deleted = ?", id, false).First(&instrument).Error
	return &instrument, err
}

func GetInstrumentByIDIncludingDeleted(db *gorm.DB, id string) (*Instrument, error) {
	var instrument Instrument
	err := db.Where("id = ?", id).First(&instrument).Error
	return &instrument, err
}

func GetInstrumentsPaginated(db *gorm.DB, page, limit int, includeDeleted bool) ([]Instrument, int64, error) {
	query := db.Model(&Instrument{})
	if !includeDeleted {
		query = query.Where("is_deleted = ?", false)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var instruments []Instrument
	offset := (page - 1) * limit
	err := query.Order("created_at DESC").Offset(offset).Limit(limit).Find(&instruments).Error
	return instruments, total, err
}

func UpdateInstrumentFields(db *gorm.DB, id string, updates map[string]interface{}) error {
	return db.Model(&Instrument{}).Where("id = ? AND is_deleted = ?", id, false).Updates(updates).Error
}

func SoftDeleteInstrumentByID(db *gorm.DB, id string) error {
	return db.Model(&Instrument{}).
		Where("id = ? AND is_deleted = ?", id, false).
		Updates(map[string]interface{}{"is_deleted": true, "status": InstrumentStatusInactive}).Error
}
