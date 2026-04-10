package config

import (
	"fmt"
	"log"
	"strings"

	"feedprovider/middleware"
	"feedprovider/models"

	"gorm.io/gorm"
)

func SeedSecurityData(db *gorm.DB) {
	seedDefaultSuperAdmin(db)
	seedDefaultAdminPermissions(db)
}

func seedDefaultSuperAdmin(db *gorm.DB) {
	exists, err := models.SuperAdminExists(db)
	if err != nil {
		log.Printf("failed to check super admin existence: %v", err)
		return
	}

	if exists {
		log.Println("super admin user already exists, skipping seed")
		return
	}

	superAdminUsername := strings.TrimSpace(App.Security.SuperAdminUsername)
	superAdminPassword := strings.TrimSpace(App.Security.SuperAdminPassword)
	if superAdminUsername == "" || superAdminPassword == "" {
		log.Println("super admin seed skipped: set SUPER_ADMIN_USERNAME/SUPER_ADMIN_PASSWORD in .env")
		return
	}

	superAdmin, err := models.CreateUserWithPassword(db, superAdminUsername, superAdminPassword, models.RoleSuperAdmin)
	if err != nil {
		log.Printf("failed to create default super admin: %v", err)
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("DEFAULT SUPER ADMIN CREATED")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Email:      %s\n", superAdmin.Username)
	fmt.Printf("Password:   %s\n", superAdminPassword)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("Use /auth/login with these credentials to get super admin JWT token")
	fmt.Println(strings.Repeat("=", 80) + "\n")
}

func seedDefaultAdminPermissions(db *gorm.DB) {
	admins, err := models.GetUsersByRoles(db, models.RoleAdmin)
	if err != nil {
		log.Printf("failed to load admin users for permission seed: %v", err)
		return
	}
	for _, admin := range admins {
		if err := models.GrantPermissions(db, admin.ID, middleware.DefaultAdminPermissions); err != nil {
			log.Printf("failed to seed permissions for admin %s: %v", admin.ID, err)
		}
	}
}
