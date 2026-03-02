package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"feedprovider/config"
	"feedprovider/services"

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

	socketHub := config.NewSocketHub()

	defer func() {
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	app := fiber.New()
	socketHub.RegisterRoutes(app, "/ws")

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
