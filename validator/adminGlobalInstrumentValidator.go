package validator

import (
	"errors"
	"feedprovider/models"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

var allowedGlobalInstrumentStatuses = map[string]struct{}{
	models.GlobalInstrumentStatusActive:   {},
	models.GlobalInstrumentStatusInactive: {},
}

var allowedGlobalInstrumentSegments = map[string]struct{}{
	models.GlobalInstrumentSegmentOthers:  {},
	models.GlobalInstrumentSegmentUSStock: {},
	models.GlobalInstrumentSegmentComex:   {},
	models.GlobalInstrumentSegmentCrypto:  {},
	models.GlobalInstrumentSegmentForex:   {},
	models.GlobalInstrumentSegmentGift:    {},
}

var allowedGlobalInstrumentExchanges = map[string]struct{}{
	models.GlobalInstrumentExchangeOthers:  {},
	models.GlobalInstrumentExchangeUSStock: {},
	models.GlobalInstrumentExchangeComex:   {},
	models.GlobalInstrumentExchangeCrypto:  {},
	models.GlobalInstrumentExchangeForex:   {},
	models.GlobalInstrumentExchangeGift:    {},
}

func normalizeGlobalInstrumentStatus(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func ValidateGlobalInstrument(inst *models.GlobalInstrument) error {
	inst.TradingSymbol = strings.TrimSpace(inst.TradingSymbol)
	inst.SubscribeSymbolName = strings.ToUpper(strings.TrimSpace(inst.SubscribeSymbolName))
	if strings.TrimSpace(inst.Symbol) == "" {
		inst.Symbol = inst.SubscribeSymbolName
	}
	if strings.TrimSpace(inst.Symbol) == "" {
		inst.Symbol = strings.ToUpper(inst.TradingSymbol)
	}
	if strings.TrimSpace(inst.Symbol) == "" {
		return errors.New("symbol is required")
	}

	inst.Symbol = strings.ToUpper(strings.TrimSpace(inst.Symbol))
	inst.Name = strings.TrimSpace(inst.Name)
	inst.InstrumentType = strings.ToUpper(strings.TrimSpace(inst.InstrumentType))
	inst.Status = normalizeGlobalInstrumentStatus(inst.Status)
	inst.Segment = models.NormalizeGlobalInstrumentSegment(inst.Segment)
	inst.Exchange = models.NormalizeGlobalInstrumentExchange(inst.Exchange)

	if _, ok := allowedGlobalInstrumentStatuses[inst.Status]; !ok {
		return errors.New("status must be ACTIVE or INACTIVE")
	}
	if _, ok := allowedGlobalInstrumentSegments[inst.Segment]; !ok {
		return errors.New("segment must be one of: OTHERS, USSTOCK, COMEX, CRYPTO, FOREX, GIFT")
	}
	if _, ok := allowedGlobalInstrumentExchanges[inst.Exchange]; !ok {
		return errors.New("exchange must be one of: OTHERS, USSTOCK, COMEX, CRYPTO, FOREX, GIFT")
	}
	return nil
}

type GlobalInstrumentListQueryRequest struct {
	Page           int
	Limit          int
	IncludeDeleted bool
	Status         string
	Segment        string
	Exchange       string
	InstrumentType string
	Expiry         string
	Search         string
	SortBy         string
	SortOrder      string
}

var allowedGlobalSortFields = map[string]struct{}{
	"id":                    {},
	"instrument_token":      {},
	"exchange_token":        {},
	"trading_symbol":        {},
	"tradingsymbol":         {},
	"name":                  {},
	"subscribe_symbol_name": {},
	"symbol":                {},
	"last_price":            {},
	"expiry":                {},
	"tick_size":             {},
	"lot_size":              {},
	"instrument_type":       {},
	"segment":               {},
	"exchange":              {},
	"strike":                {},
	"status":                {},
	"is_deleted":            {},
	"created_at":            {},
	"updated_at":            {},
	"deleted_at":            {},
}

func ValidateListGlobalInstrumentsQuery(c *fiber.Ctx) error {
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
		if parsed > 5000 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be less than or equal to 5000",
				"error":       "limit must be less than or equal to 5000",
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

	status := ""
	if rawStatus := strings.TrimSpace(c.Query("status")); rawStatus != "" {
		status = normalizeGlobalInstrumentStatus(rawStatus)
		if _, ok := allowedGlobalInstrumentStatuses[status]; !ok {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "status must be ACTIVE or INACTIVE",
				"error":       "status must be ACTIVE or INACTIVE",
			})
		}
	}

	segment := ""
	if rawSegment := strings.TrimSpace(c.Query("segment")); rawSegment != "" {
		segment = strings.ToUpper(rawSegment)
		if _, ok := allowedGlobalInstrumentSegments[segment]; !ok {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "segment must be one of: OTHERS, USSTOCK, COMEX, CRYPTO, FOREX, GIFT",
				"error":       "segment must be one of: OTHERS, USSTOCK, COMEX, CRYPTO, FOREX, GIFT",
			})
		}
	}

	exchange := ""
	if rawExchange := strings.TrimSpace(c.Query("exchange")); rawExchange != "" {
		exchange = strings.ToUpper(rawExchange)
		if _, ok := allowedGlobalInstrumentExchanges[exchange]; !ok {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "exchange must be one of: OTHERS, USSTOCK, COMEX, CRYPTO, FOREX, GIFT",
				"error":       "exchange must be one of: OTHERS, USSTOCK, COMEX, CRYPTO, FOREX, GIFT",
			})
		}
	}

	instrumentType := strings.ToUpper(strings.TrimSpace(c.Query("instrument_type")))
	expiry := ""
	if rawExpiry := strings.TrimSpace(c.Query("expiry")); rawExpiry != "" {
		var parsed time.Time
		var parseErr error
		layouts := []string{"2006-01-02", "02-01-2006", "02-01-06", time.RFC3339}
		for _, layout := range layouts {
			parsed, parseErr = time.Parse(layout, rawExpiry)
			if parseErr == nil {
				expiry = parsed.Format("2006-01-02")
				break
			}
		}
		if expiry == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "expiry must be in YYYY-MM-DD, DD-MM-YYYY, DD-MM-YY, or RFC3339 format",
				"error":       "expiry must be in YYYY-MM-DD, DD-MM-YYYY, DD-MM-YY, or RFC3339 format",
			})
		}
	}
	search := strings.TrimSpace(c.Query("search"))
	sortBy := strings.ToLower(strings.TrimSpace(c.Query("sort_by")))
	sortOrder := strings.ToLower(strings.TrimSpace(c.Query("sort_order")))

	if len(search) > 100 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "search must be less than or equal to 100 characters",
			"error":       "search must be less than or equal to 100 characters",
		})
	}

	if sortBy != "" {
		if _, ok := allowedGlobalSortFields[sortBy]; !ok {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "sort_by is invalid",
				"error":       "sort_by is invalid",
			})
		}
	}

	if sortOrder == "" {
		sortOrder = "desc"
	} else if sortOrder != "asc" && sortOrder != "desc" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sort_order must be asc or desc",
			"error":       "sort_order must be asc or desc",
		})
	}

	c.Locals("validated_global_instrument_list_query", GlobalInstrumentListQueryRequest{
		Page:           page,
		Limit:          limit,
		IncludeDeleted: includeDeleted,
		Status:         status,
		Segment:        segment,
		Exchange:       exchange,
		InstrumentType: instrumentType,
		Expiry:         expiry,
		Search:         search,
		SortBy:         sortBy,
		SortOrder:      sortOrder,
	})

	return c.Next()
}

func ValidateImportGlobalInstruments(c *fiber.Ctx) error {
	contentType := strings.ToLower(strings.TrimSpace(c.Get("Content-Type")))
	if !strings.Contains(contentType, "multipart/form-data") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "content-type must be multipart/form-data",
			"error":       "content-type must be multipart/form-data",
		})
	}

	form, err := c.MultipartForm()
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid multipart form-data",
			"error":       "invalid multipart form-data",
		})
	}

	files, ok := form.File["file"]
	if !ok || len(files) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "file is required in form-data",
			"error":       "file is required in form-data",
		})
	}

	if len(files) != 1 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "only single file upload is allowed",
			"error":       "only single file upload is allowed",
		})
	}

	totalFiles := 0
	for _, fileHeaders := range form.File {
		totalFiles += len(fileHeaders)
	}
	if totalFiles != 1 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "only one file is allowed in request",
			"error":       "only one file is allowed in request",
		})
	}

	fileName := strings.TrimSpace(files[0].Filename)
	ext := strings.ToLower(filepath.Ext(fileName))
	if ext != ".csv" && ext != ".xlsx" && ext != ".xlsm" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "only csv, xlsx, and xlsm file upload is allowed",
			"error":       "only csv, xlsx, and xlsm file upload is allowed",
		})
	}

	return c.Next()
}
