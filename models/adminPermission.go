package models

import (
	"strings"
	"time"

	"gorm.io/gorm"
)

type AdminPermission struct {
	ID         string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	UserID     string    `gorm:"type:uuid;index:idx_admin_permissions_user_perm,unique;not null" json:"user_id"`
	Permission string    `gorm:"type:text;index:idx_admin_permissions_user_perm,unique;not null" json:"permission"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func normalizePermission(permission string) string {
	return strings.ToLower(strings.TrimSpace(permission))
}

func UserHasPermission(db *gorm.DB, userID, permission string) (bool, error) {
	permission = normalizePermission(permission)
	if strings.TrimSpace(userID) == "" || permission == "" {
		return false, nil
	}

	var count int64
	err := db.Model(&AdminPermission{}).
		Where("user_id = ? AND permission = ?", userID, permission).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func GrantPermission(db *gorm.DB, userID, permission string) error {
	permission = normalizePermission(permission)
	if strings.TrimSpace(userID) == "" || permission == "" {
		return nil
	}
	entry := AdminPermission{UserID: userID, Permission: permission}
	return db.Where("user_id = ? AND permission = ?", entry.UserID, entry.Permission).
		FirstOrCreate(&entry).Error
}

func GrantPermissions(db *gorm.DB, userID string, permissions []string) error {
	for _, permission := range permissions {
		if err := GrantPermission(db, userID, permission); err != nil {
			return err
		}
	}
	return nil
}

func ListPermissionsByUserID(db *gorm.DB, userID string) ([]string, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, nil
	}
	var permissions []string
	err := db.Model(&AdminPermission{}).
		Where("user_id = ?", userID).
		Order("permission ASC").
		Pluck("permission", &permissions).Error
	return permissions, err
}
