package models

import (
	"strings"
	"time"

	"gorm.io/gorm"
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

	GlobalInstrumentSegmentOthers  = "OTHERS"
	GlobalInstrumentSegmentUSStock = "USSTOCK"
	GlobalInstrumentSegmentComex   = "COMEX"
	GlobalInstrumentSegmentCrypto  = "CRYPTO"
	GlobalInstrumentSegmentForex   = "FOREX"
	GlobalInstrumentSegmentGift    = "GIFT"

	GlobalInstrumentExchangeOthers  = "OTHERS"
	GlobalInstrumentExchangeUSStock = "USSTOCK"
	GlobalInstrumentExchangeComex   = "COMEX"
	GlobalInstrumentExchangeCrypto  = "CRYPTO"
	GlobalInstrumentExchangeForex   = "FOREX"
	GlobalInstrumentExchangeGift    = "GIFT"
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
		GlobalInstrumentSegmentComex,
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
		GlobalInstrumentExchangeComex,
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
		query = query.Where("symbol ILIKE ? OR subscribe_symbol_name ILIKE ? OR tradingsymbol ILIKE ? OR name ILIKE ?", searchPattern, searchPattern, searchPattern, searchPattern)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var instruments []GlobalInstrument
	if err := query.
		Order("created_at desc").
		Offset((page - 1) * limit).
		Limit(limit).
		Find(&instruments).Error; err != nil {
		return nil, 0, err
	}

	return instruments, total, nil
}
