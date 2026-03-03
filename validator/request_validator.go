package validator

import (
	"strings"

	"github.com/gofiber/fiber/v2"
)

// CreateUserRequest represents the request body for creating a user
type CreateUserRequest struct {
	Username string `json:"username"`
}

// UpdateStatusRequest represents the request body for updating user status
type UpdateStatusRequest struct {
	IsActive bool `json:"is_active"`
}

// RefreshTokenRequest represents the request body for refreshing a token
type RefreshTokenRequest struct {
	OldToken string `json:"old_token"`
}

// ValidateCreateUser validates the create user request
func ValidateCreateUser(c *fiber.Ctx) error {
	var req CreateUserRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}

	// Validate username
	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "username is required",
		})
	}

	if len(req.Username) < 3 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "username must be at least 3 characters",
		})
	}

	if len(req.Username) > 50 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "username must not exceed 50 characters",
		})
	}

	// Store validated request in context
	c.Locals("validated_request", req)
	return c.Next()
}

// ValidateUpdateStatus validates the update status request
func ValidateUpdateStatus(c *fiber.Ctx) error {
	var req UpdateStatusRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

// ValidateRefreshToken validates the refresh token request
func ValidateRefreshToken(c *fiber.Ctx) error {
	var req RefreshTokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}

	req.OldToken = strings.TrimSpace(req.OldToken)
	if req.OldToken == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "old_token is required",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}
