package models

import (
	"strings"
	"time"

	"gorm.io/gorm"
)

type ZerodhaSession struct {
	ID               string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	AccessToken      string    `gorm:"type:text;not null" json:"-"`
	GeneratedAt      time.Time `gorm:"not null;index" json:"generated_at"`
	UpdatedByAdminID *string   `gorm:"type:uuid;index" json:"updated_by_admin_id,omitempty"`
	UpdatedByAdmin   *User     `gorm:"foreignKey:UpdatedByAdminID" json:"-"`
	IsActive         bool      `gorm:"not null;default:true;index" json:"is_active"`
	LastError        string    `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func SaveActiveZerodhaSession(db *gorm.DB, accessToken, adminID string) (*ZerodhaSession, error) {
	accessToken = strings.TrimSpace(accessToken)
	adminID = strings.TrimSpace(adminID)
	var adminIDPtr *string
	if adminID != "" {
		adminIDCopy := adminID
		adminIDPtr = &adminIDCopy
	}

	session := &ZerodhaSession{
		AccessToken:      accessToken,
		GeneratedAt:      time.Now().UTC(),
		UpdatedByAdminID: adminIDPtr,
		IsActive:         true,
		LastError:        "",
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&ZerodhaSession{}).
			Where("is_active = ?", true).
			Updates(map[string]interface{}{"is_active": false}).Error; err != nil {
			return err
		}

		return tx.Create(session).Error
	})
	if err != nil {
		return nil, err
	}

	return GetActiveZerodhaSession(db)
}

func GetActiveZerodhaSession(db *gorm.DB) (*ZerodhaSession, error) {
	var session ZerodhaSession
	err := db.Preload("UpdatedByAdmin").
		Where("is_active = ?", true).
		Order("generated_at DESC").
		First(&session).Error
	return &session, err
}

func UpdateActiveZerodhaSessionLastError(db *gorm.DB, lastError string) error {
	return db.Model(&ZerodhaSession{}).
		Where("is_active = ?", true).
		Update("last_error", strings.TrimSpace(lastError)).Error
}

func MaskToken(token string) string {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return ""
	}
	if len(trimmed) <= 8 {
		return "********"
	}
	return trimmed[:4] + strings.Repeat("*", len(trimmed)-8) + trimmed[len(trimmed)-4:]
}
