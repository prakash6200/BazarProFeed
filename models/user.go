package models

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"gorm.io/gorm"
)

// User represents a user in the system
type User struct {
	ID               string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Username         string    `gorm:"uniqueIndex;not null" json:"username"`
	APIToken         string    `gorm:"uniqueIndex;not null" json:"api_token"`
	TokenGeneratedAt time.Time `gorm:"not null" json:"token_generated_at"`
	IsActive         bool      `gorm:"default:true" json:"is_active"`
	IsAdmin          bool      `gorm:"default:false" json:"is_admin"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// GenerateToken creates a secure random token
func GenerateToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// IsTokenExpired checks if the token is older than 24 hours
func (u *User) IsTokenExpired() bool {
	return time.Since(u.TokenGeneratedAt) > 24*time.Hour
}

// RefreshToken generates a new token for the user
func (u *User) RefreshToken(db *gorm.DB) error {
	newToken, err := GenerateToken()
	if err != nil {
		return err
	}

	u.APIToken = newToken
	u.TokenGeneratedAt = time.Now()

	return db.Save(u).Error
}

// CreateUser creates a new user with a generated token
func CreateUser(db *gorm.DB, username string, isAdmin bool) (*User, error) {
	token, err := GenerateToken()
	if err != nil {
		return nil, err
	}

	user := &User{
		Username:         username,
		APIToken:         token,
		TokenGeneratedAt: time.Now(),
		IsActive:         true,
		IsAdmin:          isAdmin,
	}

	if err := db.Create(user).Error; err != nil {
		return nil, err
	}

	return user, nil
}

// GetUserByToken retrieves a user by their API token
func GetUserByToken(db *gorm.DB, token string) (*User, error) {
	var user User
	err := db.Where("api_token = ?", token).First(&user).Error
	return &user, err
}

// GetUserByID retrieves a user by their ID
func GetUserByID(db *gorm.DB, id string) (*User, error) {
	var user User
	err := db.First(&user, "id = ?", id).Error
	return &user, err
}

// GetAllUsers retrieves all users
func GetAllUsers(db *gorm.DB) ([]User, error) {
	var users []User
	err := db.Order("created_at DESC").Find(&users).Error
	return users, err
}

// UpdateUserStatus updates the is_active status of a user
func UpdateUserStatus(db *gorm.DB, id string, isActive bool) error {
	return db.Model(&User{}).Where("id = ?", id).Update("is_active", isActive).Error
}

// DeleteUser deletes a user by ID
func DeleteUser(db *gorm.DB, id string) error {
	return db.Delete(&User{}, "id = ?", id).Error
}

// AdminExists checks if any admin user exists in the database
func AdminExists(db *gorm.DB) (bool, error) {
	var count int64
	err := db.Model(&User{}).Where("is_admin = ?", true).Count(&count).Error
	return count > 0, err
}
