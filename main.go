package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"feedprovider/config"
	"feedprovider/controller"
	"feedprovider/models"
	"feedprovider/router"
	"feedprovider/services"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

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

	adminUsername := strings.TrimSpace(os.Getenv("DEFAULT_ADMIN_USERNAME"))
	if adminUsername == "" {
		adminUsername = "admin"
	}

	adminPassword := strings.TrimSpace(os.Getenv("DEFAULT_ADMIN_PASSWORD"))
	if adminPassword == "" {
		adminPassword = "admin123"
	}

	admin, err := models.CreateUserWithPassword(db, adminUsername, adminPassword, true)
	if err != nil {
		log.Printf("failed to create default admin: %v", err)
		return
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("DEFAULT ADMIN CREATED")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Username:   %s\n", admin.Username)
	fmt.Printf("Password:   %s\n", adminPassword)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("Use /auth/login with these credentials to get admin JWT token")
	fmt.Println(strings.Repeat("=", 80) + "\n")
}

func main() {
	config.LoadConfig()
	config.ConnectDatabase()

	if err := config.DB.AutoMigrate(&models.User{}); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}
	log.Println("database migrations completed")

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

	adminController := controller.NewAdminController(config.DB, socketHub)
	userController := controller.NewUserController(config.DB, socketHub)

	router.RegisterAdminRoutes(app, adminController, config.DB)
	router.RegisterUserRoutes(app, userController, config.DB)

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
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status_code": fiber.StatusOK,
			"status":      "ok",
			"service":     "feedprovider",
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
