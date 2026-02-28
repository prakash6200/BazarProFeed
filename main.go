package main

import (
	"log"

	"feedprovider/config"

	"github.com/gofiber/fiber/v2"
)

func main() {
	config.LoadConfig()
	config.ConnectDatabase()
	defer func() {
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	app := fiber.New()

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
