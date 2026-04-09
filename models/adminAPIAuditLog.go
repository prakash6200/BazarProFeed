package models

import (
	"time"

	"gorm.io/gorm"
)

type AdminAPIAuditLog struct {
	ID         string         `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	UserID     string         `gorm:"type:uuid;index;not null" json:"user_id"`
	Username   string         `gorm:"type:text;index;not null" json:"username"`
	Role       string         `gorm:"type:user_role;index;not null" json:"role"`
	Method     string         `gorm:"type:text;index;not null" json:"method"`
	Path       string         `gorm:"type:text;index;not null" json:"path"`
	Query      string         `gorm:"type:text" json:"query"`
	StatusCode int            `gorm:"index;not null" json:"status_code"`
	IPAddress  string         `gorm:"type:text" json:"ip_address"`
	UserAgent  string         `gorm:"type:text" json:"user_agent"`
	LatencyMS  int64          `gorm:"not null" json:"latency_ms"`
	ErrorText  string         `gorm:"type:text" json:"error_text,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	DeletedAt  gorm.DeletedAt `gorm:"index" json:"-"`
}

func CreateAdminAPIAuditLog(db *gorm.DB, logEntry *AdminAPIAuditLog) error {
	if db == nil || logEntry == nil {
		return nil
	}
	return db.Create(logEntry).Error
}
