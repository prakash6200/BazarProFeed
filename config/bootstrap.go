package config

import (
	"fmt"
	"log"
	"os"
	"strings"

	"feedprovider/middleware"
	"feedprovider/models"

	"gorm.io/gorm"
)

func SeedSecurityData(db *gorm.DB) {
	seedDefaultSuperAdmin(db)
	seedDefaultAdmin(db)
	seedDefaultAdminPermissions(db)
}

func seedDefaultAdmin(db *gorm.DB) {
	exists, err := models.AdminExists(db)
	if err != nil {
		log.Printf("failed to check admin existence: %v", err)
		return
	}

	if exists {
		log.Println("admin user already exists, skipping seed")
		return
	}

	adminUsername := "admin@gmail.com"
	adminPassword := "Admin@123"

	admin, err := models.CreateUserWithPassword(db, adminUsername, adminPassword, models.RoleAdmin)
	if err != nil {
		log.Printf("failed to create default admin: %v", err)
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("DEFAULT ADMIN CREATED")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Email:      %s\n", admin.Username)
	fmt.Printf("Password:   %s\n", adminPassword)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("Use /auth/login with these credentials to get admin JWT token")
	fmt.Println(strings.Repeat("=", 80) + "\n")
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

	superAdminUsername := strings.TrimSpace(os.Getenv("SUPER_ADMIN_USERNAME"))
	superAdminPassword := strings.TrimSpace(os.Getenv("SUPER_ADMIN_PASSWORD"))
	if superAdminUsername == "" || superAdminPassword == "" {
		log.Println("super admin seed skipped: set SUPER_ADMIN_USERNAME and SUPER_ADMIN_PASSWORD in .env")
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
