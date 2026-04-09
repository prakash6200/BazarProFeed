package models

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	Segment         string     `gorm:"type:instrument_segment;not null;default:'NFO-FUT';index" json:"segment"`
	Exchange        string     `gorm:"type:instrument_exchange;not null;default:'NSE';index" json:"exchange"`
	Status          string     `gorm:"type:instrument_status;not null;default:'ACTIVE';index" json:"status"`
	IsDeleted       bool       `gorm:"not null;default:false;index" json:"is_deleted"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

const (
	InstrumentStatusActive   = "ACTIVE"
	InstrumentStatusInactive = "INACTIVE"

	InstrumentTypeCE  = "CE"
	InstrumentTypeEQ  = "EQ"
	InstrumentTypeFUT = "FUT"
	InstrumentTypePE  = "PE"

	InstrumentSegmentCDSFut = "CDS-FUT"
	InstrumentSegmentMCXFut = "MCX-FUT"
	InstrumentSegmentNFOFut = "NFO-FUT"
	InstrumentSegmentNFOOpt = "NFO-OPT"
	InstrumentSegmentEquity = "EQUITY"

	InstrumentExchangeNSE     = "NSE"
	InstrumentExchangeMCX     = "MCX"
	InstrumentExchangeMCXMini = "MCX-MINI"
	InstrumentExchangeCEPE    = "CE-PE"
	InstrumentExchangeCDS     = "CDS"
	InstrumentExchangeNSEEqu  = "NSE-EQU"
)

var allowedInstrumentSegments = map[string]struct{}{
	InstrumentSegmentNFOFut: {},
	InstrumentSegmentMCXFut: {},
	InstrumentSegmentNFOOpt: {},
	InstrumentSegmentCDSFut: {},
	InstrumentSegmentEquity: {},
}

var allowedInstrumentTypes = map[string]struct{}{
	InstrumentTypeCE:  {},
	InstrumentTypeEQ:  {},
	InstrumentTypeFUT: {},
	InstrumentTypePE:  {},
}

var allowedInstrumentExchanges = map[string]struct{}{
	InstrumentExchangeNSE:     {},
	InstrumentExchangeMCX:     {},
	InstrumentExchangeMCXMini: {},
	InstrumentExchangeCEPE:    {},
	InstrumentExchangeCDS:     {},
	InstrumentExchangeNSEEqu:  {},
}

func NormalizeInstrumentStatus(status string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(status))
	if trimmed == InstrumentStatusInactive {
		return InstrumentStatusInactive
	}
	return InstrumentStatusActive
}

func NormalizeInstrumentSegment(segment string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(segment))
	if _, ok := allowedInstrumentSegments[trimmed]; ok {
		return trimmed
	}
	return InstrumentSegmentNFOFut
}

func NormalizeInstrumentExchange(exchange string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(exchange))
	if _, ok := allowedInstrumentExchanges[trimmed]; ok {
		return trimmed
	}
	return InstrumentExchangeNSE
}

func NormalizeInstrumentType(instrumentType string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(instrumentType))
	if _, ok := allowedInstrumentTypes[trimmed]; ok {
		return trimmed
	}
	return InstrumentTypeFUT
}

func IsAllowedInstrumentSegment(segment string) bool {
	_, ok := allowedInstrumentSegments[strings.ToUpper(strings.TrimSpace(segment))]
	return ok
}

func IsAllowedInstrumentExchange(exchange string) bool {
	_, ok := allowedInstrumentExchanges[strings.ToUpper(strings.TrimSpace(exchange))]
	return ok
}

func IsAllowedInstrumentType(instrumentType string) bool {
	_, ok := allowedInstrumentTypes[strings.ToUpper(strings.TrimSpace(instrumentType))]
	return ok
}

func (i *Instrument) IsActive() bool {
	return NormalizeInstrumentStatus(i.Status) == InstrumentStatusActive
}

type CSVImportResult struct {
	TotalRows int `json:"total_rows"`
	Imported  int `json:"imported"`
	Skipped   int `json:"skipped"`
}

type InstrumentListFilters struct {
	InstrumentType string
	Segment        string
	Exchange       string
	Status         string
	ExpiryDate     string
	Search         string
	SortBy         string
	SortOrder      string
}

var allowedInstrumentSortColumns = map[string]string{
	"id":               "id",
	"instrument_token": "instrument_token",
	"exchange_token":   "exchange_token",
	"trading_symbol":   "trading_symbol",
	"tradingsymbol":    "trading_symbol",
	"name":             "name",
	"last_price":       "last_price",
	"expiry":           "expiry",
	"strike":           "strike",
	"tick_size":        "tick_size",
	"lot_size":         "lot_size",
	"instrument_type":  "instrument_type",
	"segment":          "segment",
	"exchange":         "exchange",
	"status":           "status",
	"is_deleted":       "is_deleted",
	"created_at":       "created_at",
	"updated_at":       "updated_at",
}

func buildInstrumentSortClause(filters InstrumentListFilters) string {
	column := "created_at"
	if raw := strings.ToLower(strings.TrimSpace(filters.SortBy)); raw != "" {
		if mapped, ok := allowedInstrumentSortColumns[raw]; ok {
			column = mapped
		}
	}

	direction := "DESC"
	if strings.EqualFold(strings.TrimSpace(filters.SortOrder), "asc") {
		direction = "ASC"
	}

	return column + " " + direction
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

	rawSegment := strings.TrimSpace(record[10])
	rawExchange := strings.TrimSpace(record[11])
	rawInstrumentType := strings.TrimSpace(record[9])
	if !IsAllowedInstrumentType(rawInstrumentType) {
		return nil, fmt.Errorf("invalid instrument_type: %s", rawInstrumentType)
	}
	if !IsAllowedInstrumentSegment(rawSegment) {
		return nil, fmt.Errorf("invalid segment: %s", rawSegment)
	}
	if !IsAllowedInstrumentExchange(rawExchange) {
		return nil, fmt.Errorf("invalid exchange: %s", rawExchange)
	}

	instrumentType := NormalizeInstrumentType(rawInstrumentType)
	segment := NormalizeInstrumentSegment(rawSegment)
	exchange := NormalizeInstrumentExchange(rawExchange)

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
		InstrumentType:  instrumentType,
		Segment:         segment,
		Exchange:        exchange,
		Status:          InstrumentStatusActive,
		IsDeleted:       false,
	}, nil
}

func resolveCSVPath(csvPath string) (string, error) {
	trimmed := strings.TrimSpace(csvPath)
	if trimmed == "" {
		return "", errors.New("csv path is empty")
	}

	if filepath.IsAbs(trimmed) {
		if _, err := os.Stat(trimmed); err != nil {
			return "", err
		}
		return trimmed, nil
	}

	candidates := []string{trimmed}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, trimmed))
	}
	if exePath, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exePath)
		candidates = append(candidates,
			filepath.Join(exeDir, trimmed),
			filepath.Join(exeDir, "..", trimmed),
			filepath.Join(exeDir, "..", "..", trimmed),
		)
	}

	for _, candidate := range candidates {
		resolved := filepath.Clean(candidate)
		if _, err := os.Stat(resolved); err == nil {
			return resolved, nil
		}
	}

	return "", fmt.Errorf("csv file not found for path %q", trimmed)
}

func ImportInstrumentsFromCSV(db *gorm.DB, csvPath string) (*CSVImportResult, error) {
	resolvedPath, err := resolveCSVPath(csvPath)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(resolvedPath)
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
		return nil, fmt.Errorf("invalid csv header: must be [instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,instrument_type,segment,exchange] in this exact order")
	}

	result := &CSVImportResult{}
	batch := make([]Instrument, 0, 1000)
	var rowErrors []string
	rowNum := 2 // 1-based, header is line 1

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
			rowErrors = append(rowErrors, fmt.Sprintf("row %d: read error: %v", rowNum, readErr))
			result.Skipped++
			rowNum++
			continue
		}

		result.TotalRows++
		instrument, parseErr := parseInstrumentCSVRecord(record)
		if parseErr != nil {
			rowErrors = append(rowErrors, fmt.Sprintf("row %d: %v", rowNum, parseErr))
			result.Skipped++
			rowNum++
			continue
		}

		batch = append(batch, *instrument)
		if len(batch) >= 1000 {
			if err := flush(batch); err != nil {
				return nil, err
			}
			batch = batch[:0]
		}
		rowNum++
	}

	if err := flush(batch); err != nil {
		return nil, err
	}

	if len(rowErrors) > 0 {
		return result, fmt.Errorf("%d row(s) failed to import. Errors: %v", len(rowErrors), rowErrors)
	}

	return result, nil
}

func CreateInstrument(db *gorm.DB, instrument *Instrument) error {
	instrument.Status = NormalizeInstrumentStatus(instrument.Status)
	instrument.InstrumentType = NormalizeInstrumentType(instrument.InstrumentType)
	instrument.Segment = NormalizeInstrumentSegment(instrument.Segment)
	instrument.Exchange = NormalizeInstrumentExchange(instrument.Exchange)
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

func GetInstrumentsPaginated(db *gorm.DB, page, limit int, includeDeleted bool, filters InstrumentListFilters) ([]Instrument, int64, error) {
	query := db.Model(&Instrument{})
	sortClause := buildInstrumentSortClause(filters)
	if !includeDeleted {
		query = query.Where("is_deleted = ?", false)
	}

	if strings.TrimSpace(filters.InstrumentType) != "" {
		query = query.Where("UPPER(instrument_type) = ?", strings.ToUpper(strings.TrimSpace(filters.InstrumentType)))
	}

	if strings.TrimSpace(filters.Segment) != "" {
		query = query.Where("segment = ?", NormalizeInstrumentSegment(filters.Segment))
	}

	if strings.TrimSpace(filters.Exchange) != "" {
		query = query.Where("exchange = ?", NormalizeInstrumentExchange(filters.Exchange))
	}

	if strings.TrimSpace(filters.Status) != "" {
		query = query.Where("status = ?", NormalizeInstrumentStatus(filters.Status))
	}

	if strings.TrimSpace(filters.ExpiryDate) != "" {
		query = query.Where("DATE(expiry) = ?", strings.TrimSpace(filters.ExpiryDate))
	}

	if strings.TrimSpace(filters.Search) != "" {
		searchPattern := "%" + strings.TrimSpace(filters.Search) + "%"
		query = query.Where(
			"CAST(instrument_token AS TEXT) ILIKE ? OR CAST(exchange_token AS TEXT) ILIKE ? OR trading_symbol ILIKE ? OR name ILIKE ?",
			searchPattern,
			searchPattern,
			searchPattern,
			searchPattern,
		)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var instruments []Instrument
	offset := (page - 1) * limit
	err := query.Order(sortClause).Offset(offset).Limit(limit).Find(&instruments).Error
	return instruments, total, err
}

func UpdateInstrumentFields(db *gorm.DB, id string, updates map[string]interface{}) error {
	if rawSegment, ok := updates["segment"]; ok {
		if value, ok := rawSegment.(string); ok {
			updates["segment"] = NormalizeInstrumentSegment(value)
		}
	}

	if rawInstrumentType, ok := updates["instrument_type"]; ok {
		if value, ok := rawInstrumentType.(string); ok {
			updates["instrument_type"] = NormalizeInstrumentType(value)
		}
	}

	if rawExchange, ok := updates["exchange"]; ok {
		if value, ok := rawExchange.(string); ok {
			updates["exchange"] = NormalizeInstrumentExchange(value)
		}
	}

	return db.Model(&Instrument{}).Where("id = ? AND is_deleted = ?", id, false).Updates(updates).Error
}

func SoftDeleteInstrumentByID(db *gorm.DB, id string) error {
	return db.Model(&Instrument{}).
		Where("id = ? AND is_deleted = ?", id, false).
		Updates(map[string]interface{}{"is_deleted": true, "status": InstrumentStatusInactive}).Error
}

func GetActiveInstrumentsForFeed(db *gorm.DB) ([]Instrument, error) {
	var instruments []Instrument
	err := db.
		Select("instrument_token", "trading_symbol", "exchange", "segment", "status", "is_deleted", "expiry", "strike").
		Where("is_deleted = ?", false).
		Where("status = ?", InstrumentStatusActive).
		Find(&instruments).Error
	return instruments, err
}
