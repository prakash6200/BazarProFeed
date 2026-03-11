package validator

import (
	"regexp"
	"strconv"
	"strings"
	"time"

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

type InstrumentIDRequest struct {
	ID string
}

type InstrumentListQueryRequest struct {
	Page           int
	Limit          int
	IncludeDeleted bool
	InstrumentType string
	Segment        string
	Exchange       string
	Status         string
	ExpiryDate     string
	Search         string
}

var instrumentIDPattern = regexp.MustCompile(`^[0-9a-fA-F-]{36}$`)

func ValidateCreateInstrument(c *fiber.Ctx) error {
	var req CreateInstrumentRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
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
			"message":     "instrument_token must be greater than 0",
			"error":       "instrument_token must be greater than 0",
		})
	}

	if req.ExchangeToken <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "exchange_token must be greater than 0",
			"error":       "exchange_token must be greater than 0",
		})
	}

	if req.TradingSymbol == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "tradingsymbol is required",
			"error":       "tradingsymbol is required",
		})
	}

	if req.Status != "" {
		status := strings.ToUpper(req.Status)
		if status != "ACTIVE" && status != "INACTIVE" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "status must be ACTIVE or INACTIVE",
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
			"message":     "invalid request body",
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
			"message":     "at least one field is required to update",
			"error":       "at least one field is required to update",
		})
	}

	if req.InstrumentToken != nil && *req.InstrumentToken <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "instrument_token must be greater than 0",
			"error":       "instrument_token must be greater than 0",
		})
	}

	if req.ExchangeToken != nil && *req.ExchangeToken <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "exchange_token must be greater than 0",
			"error":       "exchange_token must be greater than 0",
		})
	}

	if req.Status != nil && *req.Status != "ACTIVE" && *req.Status != "INACTIVE" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "status must be ACTIVE or INACTIVE",
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
				"message":     "invalid request body",
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

func ValidateInstrumentID(c *fiber.Ctx) error {
	id := strings.TrimSpace(c.Params("id"))
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "instrument id is required",
			"error":       "instrument id is required",
		})
	}

	if !instrumentIDPattern.MatchString(id) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid instrument id format",
			"error":       "invalid instrument id format",
		})
	}

	c.Locals("validated_instrument_id", InstrumentIDRequest{ID: id})
	return c.Next()
}

func ValidateListInstrumentsQuery(c *fiber.Ctx) error {
	page := 1
	if rawPage := strings.TrimSpace(c.Query("page")); rawPage != "" {
		parsed, err := strconv.Atoi(rawPage)
		if err != nil || parsed < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "page must be a positive integer",
				"error":       "page must be a positive integer",
			})
		}
		page = parsed
	}

	limit := 20
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be a positive integer",
				"error":       "limit must be a positive integer",
			})
		}
		if parsed > 100 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be less than or equal to 100",
				"error":       "limit must be less than or equal to 100",
			})
		}
		limit = parsed
	}

	includeDeleted := false
	if rawIncludeDeleted := strings.TrimSpace(c.Query("include_deleted")); rawIncludeDeleted != "" {
		parsed, err := strconv.ParseBool(rawIncludeDeleted)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "include_deleted must be true or false",
				"error":       "include_deleted must be true or false",
			})
		}
		includeDeleted = parsed
	}

	instrumentType := strings.ToUpper(strings.TrimSpace(c.Query("instrument_type")))
	segment := strings.ToUpper(strings.TrimSpace(c.Query("segment")))
	exchange := strings.ToUpper(strings.TrimSpace(c.Query("exchange")))
	status := strings.ToUpper(strings.TrimSpace(c.Query("status")))
	expiry := strings.TrimSpace(c.Query("expiry"))
	search := strings.TrimSpace(c.Query("search"))

	if status != "" && status != "ACTIVE" && status != "INACTIVE" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "status must be ACTIVE or INACTIVE",
			"error":       "status must be ACTIVE or INACTIVE",
		})
	}

	expiryDate := ""
	if expiry != "" {
		parsedExpiry, err := time.Parse("02-01-06", expiry)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "expiry must be in dd-mm-yy format",
				"error":       "expiry must be in dd-mm-yy format",
			})
		}
		expiryDate = parsedExpiry.Format("2006-01-02")
	}

	c.Locals("validated_instrument_list_query", InstrumentListQueryRequest{
		Page:           page,
		Limit:          limit,
		IncludeDeleted: includeDeleted,
		InstrumentType: instrumentType,
		Segment:        segment,
		Exchange:       exchange,
		Status:         status,
		ExpiryDate:     expiryDate,
		Search:         search,
	})

	return c.Next()
}
