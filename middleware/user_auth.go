package middleware

import (
	"strings"

	"feedprovider/models"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// UserAuth validates user API token
func UserAuth(db *gorm.DB) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "authorization token required",
			})
		}

		// Extract token from "Bearer <token>"
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid authorization format, use: Bearer <token>",
			})
		}

		token := parts[1]

		// Get user by token
		user, err := models.GetUserByToken(db, token)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"error": "invalid token",
				})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "authentication failed",
			})
		}

		// Check if user is active
		if !user.IsActive {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "user account is disabled",
			})
		}

		// Check if token is expired
		if user.IsTokenExpired() {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error":   "token expired",
				"message": "please refresh your token",
			})
		}

		// Store user in context for later use
		c.Locals("user", user)

		return c.Next()
	}
}
