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

type CreateInstrumentRequest struct {
	InstrumentToken int64   `json:"instrument_token"`
	ExchangeToken   int64   `json:"exchange_token"`
	TradingSymbol   string  `json:"tradingsymbol"`
	Name            string  `json:"name"`
	LastPrice       float64 `json:"last_price"`
	Expiry          string  `json:"expiry"`
	Strike          float64 `json:"strike"`
	TickSize        float64 `json:"tick_size"`
	LotSize         int     `json:"lot_size"`
	InstrumentType  string  `json:"instrument_type"`
	Segment         string  `json:"segment"`
	Exchange        string  `json:"exchange"`
}

type UpdateInstrumentRequest struct {
	InstrumentToken *int64   `json:"instrument_token"`
	ExchangeToken   *int64   `json:"exchange_token"`
	TradingSymbol   *string  `json:"tradingsymbol"`
	Name            *string  `json:"name"`
	LastPrice       *float64 `json:"last_price"`
	Expiry          *string  `json:"expiry"`
	Strike          *float64 `json:"strike"`
	TickSize        *float64 `json:"tick_size"`
	LotSize         *int     `json:"lot_size"`
	InstrumentType  *string  `json:"instrument_type"`
	Segment         *string  `json:"segment"`
	Exchange        *string  `json:"exchange"`
}

type ImportInstrumentsRequest struct {
	FilePath string `json:"file_path"`
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

func ValidateCreateInstrument(c *fiber.Ctx) error {
	var req CreateInstrumentRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "invalid request body",
		})
	}

	req.TradingSymbol = strings.TrimSpace(req.TradingSymbol)
	req.Name = strings.TrimSpace(req.Name)
	req.Expiry = strings.TrimSpace(req.Expiry)
	req.InstrumentType = strings.TrimSpace(req.InstrumentType)
	req.Segment = strings.TrimSpace(req.Segment)
	req.Exchange = strings.TrimSpace(req.Exchange)

	if req.InstrumentToken <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "instrument_token must be greater than 0",
		})
	}

	if req.ExchangeToken <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "exchange_token must be greater than 0",
		})
	}

	if req.TradingSymbol == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "tradingsymbol is required",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateUpdateInstrument(c *fiber.Ctx) error {
	var req UpdateInstrumentRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "invalid request body",
		})
	}

	if req.TradingSymbol != nil {
		trimmed := strings.TrimSpace(*req.TradingSymbol)
		req.TradingSymbol = &trimmed
	}

	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		req.Name = &trimmed
	}

	if req.Expiry != nil {
		trimmed := strings.TrimSpace(*req.Expiry)
		req.Expiry = &trimmed
	}

	if req.InstrumentType != nil {
		trimmed := strings.TrimSpace(*req.InstrumentType)
		req.InstrumentType = &trimmed
	}

	if req.Segment != nil {
		trimmed := strings.TrimSpace(*req.Segment)
		req.Segment = &trimmed
	}

	if req.Exchange != nil {
		trimmed := strings.TrimSpace(*req.Exchange)
		req.Exchange = &trimmed
	}

	hasAnyField := req.InstrumentToken != nil ||
		req.ExchangeToken != nil ||
		req.TradingSymbol != nil ||
		req.Name != nil ||
		req.LastPrice != nil ||
		req.Expiry != nil ||
		req.Strike != nil ||
		req.TickSize != nil ||
		req.LotSize != nil ||
		req.InstrumentType != nil ||
		req.Segment != nil ||
		req.Exchange != nil

	if !hasAnyField {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "at least one field is required to update",
		})
	}

	if req.InstrumentToken != nil && *req.InstrumentToken <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "instrument_token must be greater than 0",
		})
	}

	if req.ExchangeToken != nil && *req.ExchangeToken <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "exchange_token must be greater than 0",
		})
	}

	c.Locals("validated_request", req)
	return c.Next()
}

func ValidateImportInstruments(c *fiber.Ctx) error {
	var req ImportInstrumentsRequest
	if len(c.Body()) > 0 {
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"error":       "invalid request body",
			})
		}
	}

	req.FilePath = strings.TrimSpace(req.FilePath)
	if req.FilePath == "" {
		req.FilePath = "instruments.csv"
	}

	c.Locals("validated_request", req)
	return c.Next()
}
