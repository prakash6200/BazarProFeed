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

type AdminChangePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

func ValidateCreateUser(c *fiber.Ctx) error {
	var req CreateUserRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "username is required",
			"error":       "username is required",
		})
	}

	if len(req.Username) < 3 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "username must be at least 3 characters",
			"error":       "username must be at least 3 characters",
		})
	}

	if len(req.Username) > 50 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "username must not exceed 50 characters",
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
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateAdminChangePassword(c *fiber.Ctx) error {
	var req AdminChangePasswordRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	req.CurrentPassword = strings.TrimSpace(req.CurrentPassword)
	req.NewPassword = strings.TrimSpace(req.NewPassword)

	if req.CurrentPassword == "" || req.NewPassword == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "current_password and new_password are required",
			"error":       "current_password and new_password are required",
		})
	}

	if len(req.NewPassword) < 6 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "new_password must be at least 6 characters",
			"error":       "new_password must be at least 6 characters",
		})
	}

	if req.CurrentPassword == req.NewPassword {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "new_password must be different from current_password",
			"error":       "new_password must be different from current_password",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}
