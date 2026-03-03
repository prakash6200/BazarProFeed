package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"feedprovider/config"
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/models"
	"feedprovider/services"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// seedDefaultAdmin creates a default admin user if no admin exists
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

	admin, err := models.CreateUser(db, "admin", true)
	if err != nil {
		log.Printf("failed to create default admin: %v", err)
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("DEFAULT ADMIN CREATED")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Username:   %s\n", admin.Username)
	fmt.Printf("API Token:  %s\n", admin.APIToken)
	fmt.Printf("Generated:  %s\n", admin.TokenGeneratedAt.Format("2006-01-02 15:04:05"))
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("SAVE THIS TOKEN - It will be used for admin API authentication")
	fmt.Println(strings.Repeat("=", 80) + "\n")
}

func main() {
	config.LoadConfig()
	config.ConnectDatabase()

	// Run migrations
	if err := config.DB.AutoMigrate(&models.User{}); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}
	log.Println("database migrations completed")

	// Seed default admin
	seedDefaultAdmin(config.DB)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tickHub := services.NewTickHub()
	zerodhaFeedService := services.NewZerodhaFeedService(config.App.Zerodha, tickHub)
	zerodhaFeedService.Start(ctx)

	socketHub := config.NewSocketHub(config.DB)

	defer func() {
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	app := fiber.New()

	// Initialize controllers
	adminController := controller.NewAdminController(config.DB, socketHub)
	userController := controller.NewUserController(config.DB, socketHub)

	// Admin routes (protected by admin secret)
	adminRoutes := app.Group("/admin", middleware.AdminAuth)
	adminRoutes.Post("/users", validator.ValidateCreateUser, adminController.CreateUser)
	adminRoutes.Get("/users", adminController.GetAllUsers)
	adminRoutes.Get("/users/:id", adminController.GetUser)
	adminRoutes.Put("/users/:id/status", validator.ValidateUpdateStatus, adminController.UpdateUserStatus)
	adminRoutes.Delete("/users/:id", adminController.DeleteUser)
	adminRoutes.Get("/stats", adminController.GetStats)

	// User routes (protected by user token)
	userRoutes := app.Group("/api", middleware.UserAuth(config.DB))
	userRoutes.Get("/profile", userController.GetProfile)
	userRoutes.Post("/refresh-token", validator.ValidateRefreshToken, userController.RefreshToken)

	// WebSocket feed endpoint (token validation in handler)
	socketHub.RegisterRoutes(app, "/feed")

	events, unsubscribe := tickHub.Subscribe(1024)
	defer unsubscribe()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-events:
				if !ok {
					return
				}
				socketHub.BroadcastJSON(event)
			}
		}
	}()

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":  "ok",
			"service": "feedprovider",
		})
	})

	go func() {
		<-ctx.Done()
		socketHub.CloseAll()
		if err := app.Shutdown(); err != nil {
			log.Printf("failed to shutdown fiber app: %v", err)
		}
	}()

	if err := app.Listen(config.App.Server.Host + ":" + config.App.Server.Port); err != nil {
		if ctx.Err() == nil {
			log.Fatal(err)
		}
	}
}
