package validator

import (
	"strings"

	"github.com/gofiber/fiber/v2"
)

type CreateUserRequest struct {
	Username string `json:"username"`
}

type UpdateStatusRequest struct {
	IsActive bool `json:"is_active"`
}

func ValidateCreateUser(c *fiber.Ctx) error {
	var req CreateUserRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "invalid request body",
		})
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "username is required",
		})
	}

	if len(req.Username) < 3 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "username must be at least 3 characters",
		})
	}

	if len(req.Username) > 50 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "username must not exceed 50 characters",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateUpdateStatus(c *fiber.Ctx) error {
	var req UpdateStatusRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "invalid request body",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}
