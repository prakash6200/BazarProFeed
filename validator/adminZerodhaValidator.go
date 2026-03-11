package validator

import (
	"strings"

	"github.com/gofiber/fiber/v2"
)

type UpdateZerodhaSessionRequest struct {
	RequestToken string `json:"request_token"`
}

func ValidateUpdateZerodhaSession(c *fiber.Ctx) error {
	var req UpdateZerodhaSessionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	req.RequestToken = strings.TrimSpace(req.RequestToken)
	if req.RequestToken == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "request_token is required",
			"error":       "request_token is required",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}
