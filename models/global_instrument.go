package models

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GlobalInstrument struct {
	ID                  string         `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	InstrumentToken     int64          `gorm:"index" json:"instrument_token"`
	ExchangeToken       int64          `gorm:"index" json:"exchange_token"`
	TradingSymbol       string         `gorm:"index" json:"tradingsymbol"`
	Name                string         `json:"name"`
	SubscribeSymbolName string         `gorm:"index" json:"subscribe_symbol_name"`
	Symbol              string         `gorm:"uniqueIndex;not null" json:"symbol"`
	LastPrice           float64        `json:"last_price"`
	Expiry              *time.Time     `json:"expiry"`
	TickSize            float64        `json:"tick_size"`
	LotSize             int            `json:"lot_size"`
	InstrumentType      string         `json:"instrument_type"`
	Segment             string         `gorm:"type:global_market_segment;not null;default:'OTHERS';index" json:"segment"`
	Exchange            string         `gorm:"type:global_market_exchange;not null;default:'OTHERS';index" json:"exchange"`
	Strike              float64        `json:"strike"`
	Status              string         `gorm:"type:instrument_status;not null;default:'ACTIVE';index" json:"status"`
	IsDeleted           bool           `gorm:"not null;default:false;index" json:"is_deleted"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
	DeletedAt           gorm.DeletedAt `gorm:"index" json:"-"`
}

const (
	GlobalInstrumentStatusActive   = "ACTIVE"
	GlobalInstrumentStatusInactive = "INACTIVE"

	GlobalInstrumentSegmentOthers    = "OTHERS"
	GlobalInstrumentSegmentUSStock   = "USSTOCK"
	GlobalInstrumentSegmentComexFut  = "COMEX-FUT"
	GlobalInstrumentSegmentComexSpot = "COMEX-SPOT"
	GlobalInstrumentSegmentCrypto    = "CRYPTO"
	GlobalInstrumentSegmentForex     = "FOREX"
	GlobalInstrumentSegmentGift      = "GIFT"

	GlobalInstrumentExchangeOthers    = "OTHERS"
	GlobalInstrumentExchangeUSStock   = "USSTOCK"
	GlobalInstrumentExchangeComexFut  = "COMEX-FUT"
	GlobalInstrumentExchangeComexSpot = "COMEX-SPOT"
	GlobalInstrumentExchangeCrypto    = "CRYPTO"
	GlobalInstrumentExchangeForex     = "FOREX"
	GlobalInstrumentExchangeGift      = "GIFT"
)

func (i *GlobalInstrument) IsActive() bool {
	return i.Status == GlobalInstrumentStatusActive
}

type GlobalInstrumentListFilters struct {
	Status         string
	Segment        string
	Exchange       string
	InstrumentType string
	Expiry         string
	Search         string
	SortBy         string
	SortOrder      string
}

var allowedGlobalInstrumentSortColumns = map[string]string{
	"id":                    "id",
	"instrument_token":      "instrument_token",
	"exchange_token":        "exchange_token",
	"trading_symbol":        "trading_symbol",
	"tradingsymbol":         "trading_symbol",
	"name":                  "name",
	"subscribe_symbol_name": "subscribe_symbol_name",
	"symbol":                "symbol",
	"last_price":            "last_price",
	"expiry":                "expiry",
	"tick_size":             "tick_size",
	"lot_size":              "lot_size",
	"instrument_type":       "instrument_type",
	"segment":               "segment",
	"exchange":              "exchange",
	"strike":                "strike",
	"status":                "status",
	"is_deleted":            "is_deleted",
	"created_at":            "created_at",
	"updated_at":            "updated_at",
	"deleted_at":            "deleted_at",
}

func buildGlobalInstrumentSortClause(filters GlobalInstrumentListFilters) string {
	column := "created_at"
	if raw := strings.ToLower(strings.TrimSpace(filters.SortBy)); raw != "" {
		if mapped, ok := allowedGlobalInstrumentSortColumns[raw]; ok {
			column = mapped
		}
	}

	direction := "DESC"
	if strings.EqualFold(strings.TrimSpace(filters.SortOrder), "asc") {
		direction = "ASC"
	}

	return column + " " + direction
}

const (
	globalInstrumentImportFormatUnknown  = ""
	globalInstrumentImportFormatSheet    = "sheet"
	globalInstrumentImportFormatExtended = "extended"
)

func detectGlobalInstrumentImportFormat(header []string) string {
	expectedExtended := []string{
		"instrument_token",
		"exchange_token",
		"tradingsymbol",
		"name",
		"subscribe_symbol_name",
		"symbol",
		"last_price",
		"expiry",
		"tick_size",
		"lot_size",
		"instrument_type",
		"segment",
		"exchange",
		"strike",
		"status",
		"is_deleted",
	}
	expectedSheet := []string{
		"instrument_token",
		"exchange_token",
		"tradingsymbol",
		"name",
		"subscribe_symbol_name",
		"last_price",
		"expiry",
		"last_price",
		"tick_size",
		"lot_size",
		"instrument_type",
		"segment",
		"exchange",
	}

	if len(header) == len(expectedExtended) {
		for i := range expectedExtended {
			if header[i] != expectedExtended[i] {
				return globalInstrumentImportFormatUnknown
			}
		}
		return globalInstrumentImportFormatExtended
	}

	if len(header) == len(expectedSheet) {
		for i := range expectedSheet {
			if header[i] != expectedSheet[i] {
				return globalInstrumentImportFormatUnknown
			}
		}
		return globalInstrumentImportFormatSheet
	}

	return globalInstrumentImportFormatUnknown
}

func validateGlobalInstrumentImportHeader(header []string) bool {
	return detectGlobalInstrumentImportFormat(header) != globalInstrumentImportFormatUnknown
}

func parseCSVBool(value string) (bool, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false, nil
	}
	return strconv.ParseBool(trimmed)
}

func isEmptyGlobalInstrumentImportRow(record []string) bool {
	for _, value := range record {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func normalizeGlobalInstrumentImportRecord(record []string) []string {
	const expectedColumns = 16
	normalized := make([]string, expectedColumns)
	copy(normalized, record)
	return normalized
}

func trimTrailingEmptyCells(values []string) []string {
	end := len(values)
	for end > 0 && strings.TrimSpace(values[end-1]) == "" {
		end--
	}
	return values[:end]
}

func parseGlobalInstrumentImportExpiry(value string) (*time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}

	layouts := []string{"02-01-06", "02/01/06", "2006-01-02", "02-01-2006", "02/01/2006"}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			return &parsed, nil
		}
	}

	return nil, fmt.Errorf("invalid expiry format: %s", trimmed)
}

func parseGlobalInstrumentImportRecord(record []string, format string) (*GlobalInstrument, error) {
	if format == globalInstrumentImportFormatUnknown {
		return nil, errors.New("unknown import file format")
	}

	if len(record) < 13 {
		return nil, errors.New("invalid file row length")
	}

	instrumentToken, err := parseCSVInt64(record[0])
	if err != nil {
		return nil, err
	}

	exchangeToken, err := parseCSVInt64(record[1])
	if err != nil {
		return nil, err
	}

	isExtendedFormat := format == globalInstrumentImportFormatExtended
	lastPriceIndex := 5
	expiryIndex := 6
	strikeIndex := 7
	tickSizeIndex := 8
	lotSizeIndex := 9
	instrumentTypeIndex := 10
	segmentIndex := 11
	exchangeIndex := 12
	if isExtendedFormat {
		lastPriceIndex = 6
		expiryIndex = 7
		strikeIndex = 13
		tickSizeIndex = 8
		lotSizeIndex = 9
		instrumentTypeIndex = 10
		segmentIndex = 11
		exchangeIndex = 12
	}

	lastPrice, err := parseCSVFloat(record[lastPriceIndex])
	if err != nil {
		return nil, err
	}

	expiry, err := parseGlobalInstrumentImportExpiry(record[expiryIndex])
	if err != nil {
		return nil, err
	}

	tickSize, err := parseCSVFloat(record[tickSizeIndex])
	if err != nil {
		return nil, err
	}

	lotSize, err := parseCSVInt(record[lotSizeIndex])
	if err != nil {
		return nil, err
	}

	strike, err := parseCSVFloat(record[strikeIndex])
	if err != nil {
		return nil, err
	}

	isDeleted := false
	if isExtendedFormat {
		isDeleted, err = parseCSVBool(record[15])
		if err != nil {
			return nil, err
		}
	}

	status := GlobalInstrumentStatusActive
	if isExtendedFormat {
		status = NormalizeGlobalInstrumentStatus(record[14])
	}

	subscribeSymbolName := strings.ToUpper(strings.TrimSpace(record[4]))
	symbol := subscribeSymbolName
	if isExtendedFormat {
		symbol = strings.ToUpper(strings.TrimSpace(record[5]))
	}

	inst := &GlobalInstrument{
		InstrumentToken:     instrumentToken,
		ExchangeToken:       exchangeToken,
		TradingSymbol:       strings.TrimSpace(record[2]),
		Name:                strings.TrimSpace(record[3]),
		SubscribeSymbolName: subscribeSymbolName,
		Symbol:              symbol,
		LastPrice:           lastPrice,
		Expiry:              expiry,
		TickSize:            tickSize,
		LotSize:             lotSize,
		InstrumentType:      strings.ToUpper(strings.TrimSpace(record[instrumentTypeIndex])),
		Segment:             NormalizeGlobalInstrumentSegment(record[segmentIndex]),
		Exchange:            NormalizeGlobalInstrumentExchange(record[exchangeIndex]),
		Strike:              strike,
		Status:              status,
		IsDeleted:           isDeleted,
	}

	if strings.TrimSpace(inst.Symbol) == "" {
		inst.Symbol = inst.SubscribeSymbolName
	}
	if strings.TrimSpace(inst.Symbol) == "" {
		inst.Symbol = strings.ToUpper(inst.TradingSymbol)
	}
	if strings.TrimSpace(inst.Symbol) == "" {
		return nil, errors.New("symbol is required")
	}

	inst.Symbol = strings.ToUpper(strings.TrimSpace(inst.Symbol))
	return inst, nil
}

func importGlobalInstrumentsRecords(db *gorm.DB, records [][]string, format string) (*CSVImportResult, error) {
	result := &CSVImportResult{}
	batch := make([]GlobalInstrument, 0, 1000)
	rowErrors := make([]string, 0)

	flush := func(rows []GlobalInstrument) error {
		if len(rows) == 0 {
			return nil
		}

		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "symbol"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"instrument_token",
				"exchange_token",
				"trading_symbol",
				"name",
				"subscribe_symbol_name",
				"last_price",
				"expiry",
				"tick_size",
				"lot_size",
				"instrument_type",
				"segment",
				"exchange",
				"strike",
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

	for index, record := range records {
		if isEmptyGlobalInstrumentImportRow(record) {
			continue
		}

		result.TotalRows++
		instrument, err := parseGlobalInstrumentImportRecord(normalizeGlobalInstrumentImportRecord(record), format)
		if err != nil {
			rowErrors = append(rowErrors, fmt.Sprintf("row %d: %v", index+2, err))
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

	if len(rowErrors) > 0 {
		return result, fmt.Errorf("%d row(s) failed to import. Errors: %v", len(rowErrors), rowErrors)
	}

	return result, nil
}

func importGlobalInstrumentsFromCSV(db *gorm.DB, csvPath string) (*CSVImportResult, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors.New("uploaded file is empty")
	}

	header := normalizeHeader(trimTrailingEmptyCells(records[0]))
	format := detectGlobalInstrumentImportFormat(header)
	if format == globalInstrumentImportFormatUnknown {
		return nil, fmt.Errorf("invalid file header: expected either [instrument_token,exchange_token,tradingsymbol,name,subscribe_symbol_name,last_price,expiry,last_price,tick_size,lot_size,instrument_type,segment,exchange] or [instrument_token,exchange_token,tradingsymbol,name,subscribe_symbol_name,symbol,last_price,expiry,tick_size,lot_size,instrument_type,segment,exchange,strike,status,is_deleted] in this exact order")
	}

	return importGlobalInstrumentsRecords(db, records[1:], format)
}

func importGlobalInstrumentsFromExcel(db *gorm.DB, excelPath string) (*CSVImportResult, error) {
	workbook, err := excelize.OpenFile(excelPath)
	if err != nil {
		return nil, err
	}
	defer workbook.Close()

	sheets := workbook.GetSheetList()
	if len(sheets) == 0 {
		return nil, errors.New("uploaded excel file does not contain any sheet")
	}

	records, err := workbook.GetRows(sheets[0])
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors.New("uploaded file is empty")
	}

	header := normalizeHeader(trimTrailingEmptyCells(records[0]))
	format := detectGlobalInstrumentImportFormat(header)
	if format == globalInstrumentImportFormatUnknown {
		return nil, fmt.Errorf("invalid file header: expected either [instrument_token,exchange_token,tradingsymbol,name,subscribe_symbol_name,last_price,expiry,last_price,tick_size,lot_size,instrument_type,segment,exchange] or [instrument_token,exchange_token,tradingsymbol,name,subscribe_symbol_name,symbol,last_price,expiry,tick_size,lot_size,instrument_type,segment,exchange,strike,status,is_deleted] in this exact order")
	}

	return importGlobalInstrumentsRecords(db, records[1:], format)
}

func ImportGlobalInstrumentsFromFile(db *gorm.DB, filePath string) (*CSVImportResult, error) {
	resolvedPath, err := resolveCSVPath(filePath)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(filepath.Ext(resolvedPath)) {
	case ".csv":
		return importGlobalInstrumentsFromCSV(db, resolvedPath)
	case ".xlsx", ".xlsm":
		return importGlobalInstrumentsFromExcel(db, resolvedPath)
	default:
		return nil, errors.New("only csv, xlsx, and xlsm file upload is allowed")
	}
}

func NormalizeGlobalInstrumentStatus(status string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(status))
	if trimmed == GlobalInstrumentStatusInactive {
		return GlobalInstrumentStatusInactive
	}
	return GlobalInstrumentStatusActive
}

func NormalizeGlobalInstrumentSegment(segment string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(segment))
	switch trimmed {
	case GlobalInstrumentSegmentUSStock,
		GlobalInstrumentSegmentComexFut,
		GlobalInstrumentSegmentComexSpot,
		GlobalInstrumentSegmentCrypto,
		GlobalInstrumentSegmentForex,
		GlobalInstrumentSegmentGift:
		return trimmed
	default:
		return GlobalInstrumentSegmentOthers
	}
}

func NormalizeGlobalInstrumentExchange(exchange string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(exchange))
	switch trimmed {
	case GlobalInstrumentExchangeUSStock,
		GlobalInstrumentExchangeComexFut,
		GlobalInstrumentExchangeComexSpot,
		GlobalInstrumentExchangeCrypto,
		GlobalInstrumentExchangeForex,
		GlobalInstrumentExchangeGift:
		return trimmed
	default:
		return GlobalInstrumentExchangeOthers
	}
}

// GetActiveGlobalInstrumentSymbols returns symbols that are ACTIVE and not deleted.
func GetActiveGlobalInstrumentSymbols(db *gorm.DB) ([]string, error) {
	var symbols []string
	err := db.Model(&GlobalInstrument{}).
		Where("status = ? AND is_deleted = ?", GlobalInstrumentStatusActive, false).
		Distinct().
		Pluck("COALESCE(NULLIF(TRIM(subscribe_symbol_name), ''), symbol)", &symbols).Error
	if err != nil {
		return nil, err
	}
	clean := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		norm := strings.ToUpper(strings.TrimSpace(sym))
		if norm == "" {
			continue
		}
		clean = append(clean, norm)
	}
	symbols = clean
	return symbols, nil
}

func GetGlobalInstrumentsPaginated(db *gorm.DB, page, limit int, includeDeleted bool, filters GlobalInstrumentListFilters) ([]GlobalInstrument, int64, error) {
	query := db.Model(&GlobalInstrument{})
	sortClause := buildGlobalInstrumentSortClause(filters)
	if !includeDeleted {
		query = query.Where("is_deleted = ?", false)
	}

	if strings.TrimSpace(filters.Status) != "" {
		query = query.Where("status = ?", NormalizeGlobalInstrumentStatus(filters.Status))
	}

	if strings.TrimSpace(filters.Segment) != "" {
		query = query.Where("segment = ?", NormalizeGlobalInstrumentSegment(filters.Segment))
	}

	if strings.TrimSpace(filters.Exchange) != "" {
		query = query.Where("exchange = ?", NormalizeGlobalInstrumentExchange(filters.Exchange))
	}

	if strings.TrimSpace(filters.InstrumentType) != "" {
		query = query.Where("instrument_type = ?", strings.ToUpper(strings.TrimSpace(filters.InstrumentType)))
	}

	if strings.TrimSpace(filters.Expiry) != "" {
		query = query.Where("DATE(expiry) = ?", strings.TrimSpace(filters.Expiry))
	}

	if strings.TrimSpace(filters.Search) != "" {
		searchPattern := "%" + strings.TrimSpace(filters.Search) + "%"
		query = query.Where("symbol ILIKE ? OR subscribe_symbol_name ILIKE ? OR trading_symbol ILIKE ? OR name ILIKE ?", searchPattern, searchPattern, searchPattern, searchPattern)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var instruments []GlobalInstrument
	if err := query.
		Order(sortClause).
		Offset((page - 1) * limit).
		Limit(limit).
		Find(&instruments).Error; err != nil {
		return nil, 0, err
	}

	return instruments, total, nil
}
