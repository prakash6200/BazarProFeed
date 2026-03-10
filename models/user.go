package models

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type User struct {
	ID               string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Username         string    `gorm:"uniqueIndex;not null" json:"username"`
	PasswordHash     string    `gorm:"type:text" json:"-"`
	APIToken         string    `gorm:"uniqueIndex;not null" json:"api_token"`
	TokenGeneratedAt time.Time `gorm:"not null" json:"token_generated_at"`
	IsActive         bool      `gorm:"default:true" json:"is_active"`
	Role             string    `gorm:"type:varchar(10);not null;default:'USER';index" json:"role"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

const (
	RoleUser  = "USER"
	RoleAdmin = "ADMIN"
)

func normalizeRole(role string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(role))
	if trimmed == RoleAdmin {
		return RoleAdmin
	}
	return RoleUser
}

func (u *User) EffectiveRole() string {
	return normalizeRole(u.Role)
}

func (u *User) IsAdminRole() bool {
	return u.EffectiveRole() == RoleAdmin
}

func HashPassword(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func GenerateToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func (u *User) IsTokenExpired() bool {
	return time.Since(u.TokenGeneratedAt) > 24*time.Hour
}

func (u *User) RefreshToken(db *gorm.DB) error {
	newToken, err := GenerateToken()
	if err != nil {
		return err
	}

	u.APIToken = newToken
	u.TokenGeneratedAt = time.Now()

	return db.Save(u).Error
}

func CreateUser(db *gorm.DB, username, role string) (*User, error) {
	token, err := GenerateToken()
	if err != nil {
		return nil, err
	}

	user := &User{
		Username:         username,
		APIToken:         token,
		TokenGeneratedAt: time.Now(),
		IsActive:         true,
		Role:             normalizeRole(role),
	}

	if err := db.Create(user).Error; err != nil {
		return nil, err
	}

	return user, nil
}

func CreateUserWithPassword(db *gorm.DB, username, password, role string) (*User, error) {
	token, err := GenerateToken()
	if err != nil {
		return nil, err
	}

	hash, err := HashPassword(password)
	if err != nil {
		return nil, err
	}

	user := &User{
		Username:         username,
		PasswordHash:     hash,
		APIToken:         token,
		TokenGeneratedAt: time.Now(),
		IsActive:         true,
		Role:             normalizeRole(role),
	}

	if err := db.Create(user).Error; err != nil {
		return nil, err
	}

	return user, nil
}

func GetUserByToken(db *gorm.DB, token string) (*User, error) {
	var user User
	err := db.Where("api_token = ?", token).First(&user).Error
	return &user, err
}

func GetUserByID(db *gorm.DB, id string) (*User, error) {
	var user User
	err := db.First(&user, "id = ?", id).Error
	return &user, err
}

func GetUserByUsername(db *gorm.DB, username string) (*User, error) {
	var user User
	err := db.Where("username = ?", username).First(&user).Error
	return &user, err
}

func GetAllUsers(db *gorm.DB) ([]User, error) {
	var users []User
	err := db.Order("created_at DESC").Find(&users).Error
	return users, err
}

func UpdateUserStatus(db *gorm.DB, id string, isActive bool) error {
	return db.Model(&User{}).Where("id = ?", id).Update("is_active", isActive).Error
}

func DeleteUser(db *gorm.DB, id string) error {
	return db.Delete(&User{}, "id = ?", id).Error
}

func AdminExists(db *gorm.DB) (bool, error) {
	var count int64
	err := db.Model(&User{}).Where("role = ?", RoleAdmin).Count(&count).Error
	return count > 0, err
}
