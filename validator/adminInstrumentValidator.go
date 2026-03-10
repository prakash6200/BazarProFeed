package validator

import (
	"strings"

	"github.com/gofiber/fiber/v2"
)

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
	Status          string  `json:"status"`
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
	Status          *string  `json:"status"`
}

type ImportInstrumentsRequest struct {
	FilePath string `json:"file_path"`
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
	req.Status = strings.TrimSpace(req.Status)

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

	if req.Status != "" {
		status := strings.ToUpper(req.Status)
		if status != "ACTIVE" && status != "INACTIVE" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"error":       "status must be ACTIVE or INACTIVE",
			})
		}
		req.Status = status
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

	if req.Status != nil {
		trimmed := strings.ToUpper(strings.TrimSpace(*req.Status))
		req.Status = &trimmed
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
		req.Exchange != nil ||
		req.Status != nil

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

	if req.Status != nil && *req.Status != "ACTIVE" && *req.Status != "INACTIVE" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "status must be ACTIVE or INACTIVE",
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
