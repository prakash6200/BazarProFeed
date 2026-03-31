package models

import (
	"strings"
	"time"

	"gorm.io/gorm"
)

type GlobalInstrument struct {
	ID        string         `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Symbol    string         `gorm:"uniqueIndex;not null" json:"symbol"`
	Name      string         `json:"name"`
	Status    string         `gorm:"type:instrument_status;not null;default:'ACTIVE';index" json:"status"`
	IsDeleted bool           `gorm:"not null;default:false;index" json:"is_deleted"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

const (
	GlobalInstrumentStatusActive   = "ACTIVE"
	GlobalInstrumentStatusInactive = "INACTIVE"
)

func (i *GlobalInstrument) IsActive() bool {
	return i.Status == GlobalInstrumentStatusActive
}

type GlobalInstrumentListFilters struct {
	Status string
	Search string
}

func NormalizeGlobalInstrumentStatus(status string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(status))
	if trimmed == GlobalInstrumentStatusInactive {
		return GlobalInstrumentStatusInactive
	}
	return GlobalInstrumentStatusActive
}

// GetActiveGlobalInstrumentSymbols returns symbols that are ACTIVE and not deleted.
func GetActiveGlobalInstrumentSymbols(db *gorm.DB) ([]string, error) {
	var symbols []string
	err := db.Model(&GlobalInstrument{}).
		Where("status = ? AND is_deleted = ?", GlobalInstrumentStatusActive, false).
		Distinct().
		Pluck("symbol", &symbols).Error
	if err != nil {
		return nil, err
	}
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

	if strings.TrimSpace(filters.Search) != "" {
		searchPattern := "%" + strings.TrimSpace(filters.Search) + "%"
		query = query.Where("symbol ILIKE ? OR name ILIKE ?", searchPattern, searchPattern)
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
