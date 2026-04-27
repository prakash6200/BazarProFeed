package validator

import (
	"path/filepath"
	"regexp"
	"sort"
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

type BulkUpdateInstrumentItem struct {
	ID string `json:"id"`
	UpdateInstrumentRequest
}

type BulkUpdateInstrumentsRequest struct {
	Updates []BulkUpdateInstrumentItem `json:"updates"`
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
	SortBy         string
	SortOrder      string
}

var allowedInstrumentSortFields = map[string]struct{}{
	"id":               {},
	"instrument_token": {},
	"exchange_token":   {},
	"trading_symbol":   {},
	"tradingsymbol":    {},
	"name":             {},
	"last_price":       {},
	"expiry":           {},
	"strike":           {},
	"tick_size":        {},
	"lot_size":         {},
	"instrument_type":  {},
	"segment":          {},
	"exchange":         {},
	"status":           {},
	"is_deleted":       {},
	"created_at":       {},
	"updated_at":       {},
}

var instrumentIDPattern = regexp.MustCompile(`^[0-9a-fA-F-]{36}$`)

var allowedInstrumentTypes = map[string]struct{}{
	"CE":  {},
	"EQ":  {},
	"FUT": {},
	"PE":  {},
}

var allowedSegments = map[string]struct{}{
	"NFO-FUT": {},
	"MCX-FUT": {},
	"NFO-OPT": {},
	"CDS-FUT": {},
	"EQUITY":  {},
}

var allowedExchanges = map[string]struct{}{
	"NSE":      {},
	"MCX":      {},
	"MCX-MINI": {},
	"CE-PE":    {},
	"CDS":      {},
	"NSE-EQU":  {},
}

func isAllowedEnumValue(value string, allowed map[string]struct{}) bool {
	_, ok := allowed[value]
	return ok
}

func allowedEnumValues(allowed map[string]struct{}) string {
	values := make([]string, 0, len(allowed))
	for value := range allowed {
		values = append(values, value)
	}
	sort.Strings(values)
	return strings.Join(values, ", ")
}

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
	req.InstrumentType = strings.ToUpper(strings.TrimSpace(req.InstrumentType))
	req.Segment = strings.ToUpper(strings.TrimSpace(req.Segment))
	req.Exchange = strings.ToUpper(strings.TrimSpace(req.Exchange))
	req.Status = strings.TrimSpace(req.Status)

	if req.InstrumentType == "" || !isAllowedEnumValue(req.InstrumentType, allowedInstrumentTypes) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
			"error":       "instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
		})
	}

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

	if req.Segment == "" || !isAllowedEnumValue(req.Segment, allowedSegments) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "segment must be one of: " + allowedEnumValues(allowedSegments),
			"error":       "segment must be one of: " + allowedEnumValues(allowedSegments),
		})
	}

	if req.Exchange == "" || !isAllowedEnumValue(req.Exchange, allowedExchanges) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "exchange must be one of: " + allowedEnumValues(allowedExchanges),
			"error":       "exchange must be one of: " + allowedEnumValues(allowedExchanges),
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
		trimmed := strings.ToUpper(strings.TrimSpace(*req.InstrumentType))
		if trimmed == "" || !isAllowedEnumValue(trimmed, allowedInstrumentTypes) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
				"error":       "instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
			})
		}
		req.InstrumentType = &trimmed
	}

	if req.Segment != nil {
		trimmed := strings.ToUpper(strings.TrimSpace(*req.Segment))
		if trimmed == "" || !isAllowedEnumValue(trimmed, allowedSegments) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "segment must be one of: " + allowedEnumValues(allowedSegments),
				"error":       "segment must be one of: " + allowedEnumValues(allowedSegments),
			})
		}
		req.Segment = &trimmed
	}

	if req.Exchange != nil {
		trimmed := strings.ToUpper(strings.TrimSpace(*req.Exchange))
		if trimmed == "" || !isAllowedEnumValue(trimmed, allowedExchanges) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "exchange must be one of: " + allowedEnumValues(allowedExchanges),
				"error":       "exchange must be one of: " + allowedEnumValues(allowedExchanges),
			})
		}
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

func ValidateBulkUpdateInstruments(c *fiber.Ctx) error {
	var req BulkUpdateInstrumentsRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	if len(req.Updates) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "updates must contain at least one item",
			"error":       "updates must contain at least one item",
		})
	}

	if len(req.Updates) > 500 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "updates must contain at most 500 items",
			"error":       "updates must contain at most 500 items",
		})
	}

	for i := range req.Updates {
		item := &req.Updates[i]
		item.ID = strings.TrimSpace(item.ID)
		if !instrumentIDPattern.MatchString(item.ID) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "updates[" + strconv.Itoa(i) + "].id must be a valid UUID",
				"error":       "updates[" + strconv.Itoa(i) + "].id must be a valid UUID",
			})
		}

		if item.TradingSymbol != nil {
			trimmed := strings.TrimSpace(*item.TradingSymbol)
			item.TradingSymbol = &trimmed
		}

		if item.Name != nil {
			trimmed := strings.TrimSpace(*item.Name)
			item.Name = &trimmed
		}

		if item.Expiry != nil {
			trimmed := strings.TrimSpace(*item.Expiry)
			item.Expiry = &trimmed
		}

		if item.InstrumentType != nil {
			trimmed := strings.ToUpper(strings.TrimSpace(*item.InstrumentType))
			if trimmed == "" || !isAllowedEnumValue(trimmed, allowedInstrumentTypes) {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"status_code": fiber.StatusBadRequest,
					"message":     "updates[" + strconv.Itoa(i) + "].instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
					"error":       "updates[" + strconv.Itoa(i) + "].instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
				})
			}
			item.InstrumentType = &trimmed
		}

		if item.Segment != nil {
			trimmed := strings.ToUpper(strings.TrimSpace(*item.Segment))
			if trimmed == "" || !isAllowedEnumValue(trimmed, allowedSegments) {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"status_code": fiber.StatusBadRequest,
					"message":     "updates[" + strconv.Itoa(i) + "].segment must be one of: " + allowedEnumValues(allowedSegments),
					"error":       "updates[" + strconv.Itoa(i) + "].segment must be one of: " + allowedEnumValues(allowedSegments),
				})
			}
			item.Segment = &trimmed
		}

		if item.Exchange != nil {
			trimmed := strings.ToUpper(strings.TrimSpace(*item.Exchange))
			if trimmed == "" || !isAllowedEnumValue(trimmed, allowedExchanges) {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"status_code": fiber.StatusBadRequest,
					"message":     "updates[" + strconv.Itoa(i) + "].exchange must be one of: " + allowedEnumValues(allowedExchanges),
					"error":       "updates[" + strconv.Itoa(i) + "].exchange must be one of: " + allowedEnumValues(allowedExchanges),
				})
			}
			item.Exchange = &trimmed
		}

		if item.Status != nil {
			trimmed := strings.ToUpper(strings.TrimSpace(*item.Status))
			item.Status = &trimmed
		}

		hasAnyField := item.InstrumentToken != nil ||
			item.ExchangeToken != nil ||
			item.TradingSymbol != nil ||
			item.Name != nil ||
			item.LastPrice != nil ||
			item.Expiry != nil ||
			item.Strike != nil ||
			item.TickSize != nil ||
			item.LotSize != nil ||
			item.InstrumentType != nil ||
			item.Segment != nil ||
			item.Exchange != nil ||
			item.Status != nil

		if !hasAnyField {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "updates[" + strconv.Itoa(i) + "] must contain at least one field to update",
				"error":       "updates[" + strconv.Itoa(i) + "] must contain at least one field to update",
			})
		}

		if item.InstrumentToken != nil && *item.InstrumentToken <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "updates[" + strconv.Itoa(i) + "].instrument_token must be greater than 0",
				"error":       "updates[" + strconv.Itoa(i) + "].instrument_token must be greater than 0",
			})
		}

		if item.ExchangeToken != nil && *item.ExchangeToken <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "updates[" + strconv.Itoa(i) + "].exchange_token must be greater than 0",
				"error":       "updates[" + strconv.Itoa(i) + "].exchange_token must be greater than 0",
			})
		}

		if item.Status != nil && *item.Status != "ACTIVE" && *item.Status != "INACTIVE" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "updates[" + strconv.Itoa(i) + "].status must be ACTIVE or INACTIVE",
				"error":       "updates[" + strconv.Itoa(i) + "].status must be ACTIVE or INACTIVE",
			})
		}
	}

	c.Locals("validated_bulk_update_request", req)
	return c.Next()
}

func ValidateImportInstruments(c *fiber.Ctx) error {
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
	if strings.ToLower(filepath.Ext(fileName)) != ".csv" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "only csv file upload is allowed",
			"error":       "only csv file upload is allowed",
		})
	}

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

	instrumentType := strings.ToUpper(strings.TrimSpace(c.Query("instrument_type")))
	segment := strings.ToUpper(strings.TrimSpace(c.Query("segment")))
	exchange := strings.ToUpper(strings.TrimSpace(c.Query("exchange")))
	status := strings.ToUpper(strings.TrimSpace(c.Query("status")))
	expiry := strings.TrimSpace(c.Query("expiry"))
	search := strings.TrimSpace(c.Query("search"))
	sortBy := strings.ToLower(strings.TrimSpace(c.Query("sort_by")))
	sortOrder := strings.ToLower(strings.TrimSpace(c.Query("sort_order")))

	if instrumentType != "" && !isAllowedEnumValue(instrumentType, allowedInstrumentTypes) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
			"error":       "instrument_type must be one of: " + allowedEnumValues(allowedInstrumentTypes),
		})
	}

	if segment != "" && !isAllowedEnumValue(segment, allowedSegments) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "segment must be one of: " + allowedEnumValues(allowedSegments),
			"error":       "segment must be one of: " + allowedEnumValues(allowedSegments),
		})
	}

	if exchange != "" && !isAllowedEnumValue(exchange, allowedExchanges) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "exchange must be one of: " + allowedEnumValues(allowedExchanges),
			"error":       "exchange must be one of: " + allowedEnumValues(allowedExchanges),
		})
	}

	if status != "" && status != "ACTIVE" && status != "INACTIVE" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "status must be ACTIVE or INACTIVE",
			"error":       "status must be ACTIVE or INACTIVE",
		})
	}

	if sortBy != "" {
		if _, ok := allowedInstrumentSortFields[sortBy]; !ok {
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
		SortBy:         sortBy,
		SortOrder:      sortOrder,
	})

	return c.Next()
}
