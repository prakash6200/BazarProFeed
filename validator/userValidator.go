package validator

import (
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

type RefreshTokenRequest struct {
	OldToken string `json:"old_token"`
}

type SignupRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type AnalyticsCandlesRequest struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	Page        int    `json:"page"`
	SizePerPage int    `json:"sizePerPage"`
}

type AnalyticsTicksRequest struct {
	Exchange      string `json:"exchange"`
	Symbol        string `json:"symbol"`
	Interval      string `json:"interval"`
	IntervalStart string `json:"interval_start"`
	Page          int    `json:"page"`
	SizePerPage   int    `json:"sizePerPage"`
}

func validateAnalyticsInterval(value string) string {
	interval := strings.ToLower(strings.TrimSpace(value))
	if interval == "" {
		return "1m"
	}
	return interval
}

func ValidateAnalyticsCandlesPayload(c *fiber.Ctx) error {
	var req AnalyticsCandlesRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	req.Exchange = strings.TrimSpace(req.Exchange)
	req.Symbol = strings.TrimSpace(req.Symbol)
	req.Interval = validateAnalyticsInterval(req.Interval)

	if req.Interval != "1m" && req.Interval != "3m" && req.Interval != "5m" && req.Interval != "15m" && req.Interval != "30m" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 3m, 5m, 15m, 30m",
			"error":       "interval must be one of: 1m, 3m, 5m, 15m, 30m",
		})
	}

	if req.Page < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "page must be greater than or equal to 0",
			"error":       "page must be greater than or equal to 0",
		})
	}

	if req.SizePerPage < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be greater than or equal to 0",
			"error":       "sizePerPage must be greater than or equal to 0",
		})
	}

	if req.SizePerPage > 1000 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be less than or equal to 1000",
			"error":       "sizePerPage must be less than or equal to 1000",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateAnalyticsTicksPayload(c *fiber.Ctx) error {
	var req AnalyticsTicksRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	req.Exchange = strings.TrimSpace(req.Exchange)
	req.Symbol = strings.TrimSpace(req.Symbol)
	req.Interval = validateAnalyticsInterval(req.Interval)
	req.IntervalStart = strings.TrimSpace(req.IntervalStart)

	if req.Interval != "1m" && req.Interval != "3m" && req.Interval != "5m" && req.Interval != "15m" && req.Interval != "30m" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 3m, 5m, 15m, 30m",
			"error":       "interval must be one of: 1m, 3m, 5m, 15m, 30m",
		})
	}

	if req.IntervalStart != "" {
		if _, err := time.Parse(time.RFC3339, req.IntervalStart); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "interval_start must be RFC3339 format",
				"error":       "interval_start must be RFC3339 format",
			})
		}
	}

	if req.Page < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "page must be greater than or equal to 0",
			"error":       "page must be greater than or equal to 0",
		})
	}

	if req.SizePerPage < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be greater than or equal to 0",
			"error":       "sizePerPage must be greater than or equal to 0",
		})
	}

	if req.SizePerPage > 1000 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be less than or equal to 1000",
			"error":       "sizePerPage must be less than or equal to 1000",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateRefreshToken(c *fiber.Ctx) error {
	var req RefreshTokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	req.OldToken = strings.TrimSpace(req.OldToken)
	if req.OldToken == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "old_token is required",
			"error":       "old_token is required",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateSignup(c *fiber.Ctx) error {
	var req SignupRequest
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

	if len(req.Username) < 2 || len(req.Username) > 50 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "username must be between 2 and 50 characters",
			"error":       "username must be between 2 and 50 characters",
		})
	}

	if len(req.Password) < 6 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "password must be at least 6 characters",
			"error":       "password must be at least 6 characters",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateLogin(c *fiber.Ctx) error {
	var req LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" || req.Password == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "username and password are required",
			"error":       "username and password are required",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}
