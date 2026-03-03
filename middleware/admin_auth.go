package middleware

import (
	"os"

	"github.com/gofiber/fiber/v2"
)

// AdminAuth validates the admin secret key
func AdminAuth(c *fiber.Ctx) error {
	adminSecret := os.Getenv("ADMIN_SECRET_KEY")
	if adminSecret == "" {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "admin authentication not configured",
		})
	}

	providedSecret := c.Get("X-Admin-Secret")
	if providedSecret == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "admin secret required in X-Admin-Secret header",
		})
	}

	if providedSecret != adminSecret {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "invalid admin secret",
		})
	}

	return c.Next()
}
