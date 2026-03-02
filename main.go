package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"feedprovider/config"
	"feedprovider/services"

	ws "github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

func main() {
	config.LoadConfig()
	config.ConnectDatabase()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tickHub := services.NewTickHub()
	zerodhaFeedService := services.NewZerodhaFeedService(config.App.Zerodha, tickHub)
	zerodhaFeedService.Start(ctx)

	defer func() {
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	app := fiber.New()

	app.Use("/ws", func(c *fiber.Ctx) error {
		if ws.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get("/ws", ws.New(func(c *ws.Conn) {
		events, unsubscribe := tickHub.Subscribe(256)
		defer unsubscribe()

		for event := range events {
			if err := c.WriteJSON(event); err != nil {
				return
			}
		}
	}))

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":  "ok",
			"service": "feedprovider",
		})
	})

	if err := app.Listen(config.App.Server.Host + ":" + config.App.Server.Port); err != nil {
		log.Fatal(err)
	}
}
