package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"feedprovider/config"
	"feedprovider/controller"
	"feedprovider/models"
	"feedprovider/router"
	"feedprovider/services"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
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

func main() {
	config.LoadConfig()
	config.ConnectDatabase()
	config.ConnectRedis()

	seedDefaultAdmin(config.DB)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	zerodhaCache := services.NewRedisTickCache(config.RedisClient)
	globalCache := services.NewRedisTickCache(config.RedisClient)

	tickHub := services.NewTickHub()
	zerodhaFeedService := services.NewZerodhaFeedService(config.App.Zerodha, config.DB, tickHub, zerodhaCache)
	zerodhaFeedService.Start(ctx)

	globMarketTickHub := services.NewTickHub()
	globMarketFeedService := services.NewGlobalMarketFeedService(config.DB, globalCache)
	globMarketFeedService.Start(ctx, globMarketTickHub)
	globSocketHub := config.NewSocketHub(config.DB)

	socketHub := config.NewSocketHub(config.DB)

	defer func() {
		config.CloseRedis()
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	app := fiber.New(fiber.Config{
		BodyLimit: 50 * 1024 * 1024,
	})

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Origin,Content-Type,Accept,Authorization",
	}))

	adminController := controller.NewAdminController(config.DB, socketHub)
	instrumentController := controller.NewAdminInstrumentController(config.DB, zerodhaFeedService)
	globalInstrumentController := controller.NewAdminGlobalInstrumentController(config.DB, globMarketFeedService)
	zerodhaController := controller.NewAdminZerodhaController(config.DB, zerodhaFeedService)
	userController := controller.NewUserController(config.DB, socketHub)

	router.RegisterAdminRoutes(app, adminController, config.DB)
	router.RegisterAdminInstrumentRoutes(app, instrumentController, config.DB)
	router.RegisterAdminGlobalInstrumentRoutes(app, globalInstrumentController, config.DB)
	router.RegisterAdminZerodhaRoutes(app, zerodhaController, config.DB)
	router.RegisterUserRoutes(app, userController, config.DB)

	socketHub.SetInitialStateGetter(func(ctx context.Context, filterSym string) []json.RawMessage {
		ticks := zerodhaCache.Snapshot(ctx, services.ZerodhaTickPrefix, filterSym)
		status := services.MarketStatusOpen
		if !services.IsIndianMarketOpen() {
			status = services.MarketStatusClosed
		}
		msgs := make([]json.RawMessage, 0, len(ticks))
		for _, tick := range ticks {
			tick.MarketStatus = status
			if raw, err := json.Marshal(tick); err == nil {
				msgs = append(msgs, raw)
			}
		}
		return msgs
	})

	globSocketHub.SetInitialStateGetter(func(ctx context.Context, filterSym string) []json.RawMessage {
		ticks := globalCache.Snapshot(ctx, services.GlobalTickPrefix, filterSym)
		status := services.MarketStatusOpen
		if !services.IsGlobalMarketOpen() {
			status = services.MarketStatusClosed
		}
		msgs := make([]json.RawMessage, 0, len(ticks))
		for _, tick := range ticks {
			tick.MarketStatus = status
			if raw, err := json.Marshal(tick); err == nil {
				msgs = append(msgs, raw)
			}
		}
		return msgs
	})

	socketHub.RegisterRoutes(app, "/feed")
	socketHub.RegisterSingleInstrumentRoutes(app, "/feed/one")
	globSocketHub.RegisterRoutes(app, "/globalfeed")
	globSocketHub.RegisterSingleInstrumentRoutes(app, "/globalfeed/one")

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

	globEvents, globUnsubscribe := globMarketTickHub.Subscribe(1024)
	defer globUnsubscribe()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-globEvents:
				if !ok {
					return
				}
				globSocketHub.BroadcastJSON(event)
			}
		}
	}()

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status_code": fiber.StatusOK,
			"message":     "service is healthy",
			"status":      "ok",
			"service":     "feedprovider",
		})
	})

	go func() {
		<-ctx.Done()
		socketHub.CloseAll()
		globSocketHub.CloseAll()
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
