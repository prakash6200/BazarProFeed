package models

import (
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type AdminPermission struct {
	ID         string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	UserID     string    `gorm:"type:uuid;index:idx_admin_permissions_user_perm,unique;not null" json:"user_id"`
	User       *User     `gorm:"foreignKey:UserID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE" json:"-"`
	Permission string    `gorm:"type:text;index:idx_admin_permissions_user_perm,unique;not null" json:"permission"`
	IsAllowed  bool      `gorm:"not null" json:"is_allowed"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type PermissionState struct {
	Permission string `json:"permission"`
	Allowed    bool   `json:"allowed"`
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
		Where("user_id = ? AND permission = ? AND is_allowed = ?", userID, permission, true).
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
	entry := AdminPermission{UserID: userID, Permission: permission, IsAllowed: true}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "permission"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"is_allowed": true, "updated_at": time.Now()}),
	}).Create(&entry).Error
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
		Where("user_id = ? AND is_allowed = ?", userID, true).
		Order("permission ASC").
		Pluck("permission", &permissions).Error
	return permissions, err
}

func ListPermissionStatesByUserID(db *gorm.DB, userID string, defaultPermissions []string) ([]PermissionState, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, nil
	}

	var rows []AdminPermission
	if err := db.Where("user_id = ?", userID).Find(&rows).Error; err != nil {
		return nil, err
	}

	stateMap := make(map[string]bool, len(rows))
	for _, row := range rows {
		stateMap[normalizePermission(row.Permission)] = row.IsAllowed
	}

	states := make([]PermissionState, 0, len(defaultPermissions))
	for _, perm := range defaultPermissions {
		norm := normalizePermission(perm)
		allowed := false
		if val, ok := stateMap[norm]; ok {
			allowed = val
		}
		states = append(states, PermissionState{Permission: norm, Allowed: allowed})
	}

	return states, nil
}

func ReplacePermissionsForUser(db *gorm.DB, userID string, permissions []string) error {
	states := make([]PermissionState, 0, len(permissions))
	for _, permission := range permissions {
		norm := normalizePermission(permission)
		if norm == "" {
			continue
		}
		states = append(states, PermissionState{Permission: norm, Allowed: true})
	}
	return ReplacePermissionStatesForUser(db, userID, states)
}

func ReplacePermissionStatesForUser(db *gorm.DB, userID string, states []PermissionState) error {
	if strings.TrimSpace(userID) == "" {
		return nil
	}

	tx := db.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	if err := tx.Where("user_id = ?", userID).Delete(&AdminPermission{}).Error; err != nil {
		_ = tx.Rollback().Error
		return err
	}

	for _, state := range states {
		norm := normalizePermission(state.Permission)
		if norm == "" {
			continue
		}
		entry := AdminPermission{UserID: userID, Permission: norm, IsAllowed: state.Allowed}
		if err := tx.Create(&entry).Error; err != nil {
			_ = tx.Rollback().Error
			return err
		}
	}

	if err := tx.Commit().Error; err != nil {
		_ = tx.Rollback().Error
		return err
	}

	return nil
}
