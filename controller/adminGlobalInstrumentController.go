package controller

import (
	"context"
	"errors"
	"feedprovider/models"
	"feedprovider/services"
	"feedprovider/validator"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type AdminGlobalInstrumentController struct {
	DB      *gorm.DB
	FeedSvc services.MarketFeedProvider
}

type globalInstrumentUpsertRequest struct {
	InstrumentToken     int64   `json:"instrument_token"`
	ExchangeToken       int64   `json:"exchange_token"`
	TradingSymbol       string  `json:"tradingsymbol"`
	Name                string  `json:"name"`
	SubscribeSymbolName string  `json:"subscribe_symbol_name"`
	Symbol              string  `json:"symbol"`
	LastPrice           float64 `json:"last_price"`
	Expiry              string  `json:"expiry"`
	TickSize            float64 `json:"tick_size"`
	LotSize             int     `json:"lot_size"`
	InstrumentType      string  `json:"instrument_type"`
	Segment             string  `json:"segment"`
	Exchange            string  `json:"exchange"`
	Strike              float64 `json:"strike"`
	Status              string  `json:"status"`
	IsDeleted           bool    `json:"is_deleted"`
}

func NewAdminGlobalInstrumentController(db *gorm.DB, feedSvc services.MarketFeedProvider) *AdminGlobalInstrumentController {
	return &AdminGlobalInstrumentController{DB: db, FeedSvc: feedSvc}
}

func parseGlobalInstrumentExpiry(value string) (*time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}

	parsed, err := time.Parse("02-01-06", trimmed)
	if err != nil {
		return nil, err
	}

	return &parsed, nil
}

func (ctl *AdminGlobalInstrumentController) refreshFeed() error {
	if ctl.FeedSvc == nil {
		return nil
	}
	return ctl.FeedSvc.RefreshFromDatabase(context.Background())
}

func (ctl *AdminGlobalInstrumentController) List(c *fiber.Ctx) error {
	query := c.Locals("validated_global_instrument_list_query").(validator.GlobalInstrumentListQueryRequest)
	page := query.Page
	limit := query.Limit
	includeDeleted := query.IncludeDeleted
	filters := models.GlobalInstrumentListFilters{
		Status:         query.Status,
		Segment:        query.Segment,
		Exchange:       query.Exchange,
		InstrumentType: query.InstrumentType,
		Expiry:         query.Expiry,
		Search:         query.Search,
		SortBy:         query.SortBy,
		SortOrder:      query.SortOrder,
	}

	instruments, total, err := models.GetGlobalInstrumentsPaginated(ctl.DB, page, limit, includeDeleted, filters)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch global instruments",
			"error":       "failed to fetch global instruments",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(limit) - 1) / int64(limit))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "global instruments fetched successfully",
		"instruments": instruments,
		"pagination": fiber.Map{
			"page":            page,
			"limit":           limit,
			"total_records":   total,
			"total_pages":     totalPages,
			"include_deleted": includeDeleted,
			"filters": fiber.Map{
				"status":          query.Status,
				"segment":         query.Segment,
				"exchange":        query.Exchange,
				"instrument_type": query.InstrumentType,
				"expiry":          query.Expiry,
				"search":          query.Search,
				"sort_by":         query.SortBy,
				"sort_order":      query.SortOrder,
			},
		},
	})
}

func (ctl *AdminGlobalInstrumentController) Import(c *fiber.Ctx) error {
	uploadFile, err := c.FormFile("file")
	if err != nil || uploadFile == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "file is required in form-data",
			"error":       "file is required in form-data",
		})
	}

	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(uploadFile.Filename)))
	if ext != ".csv" && ext != ".xlsx" && ext != ".xlsm" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "only csv, xlsx, and xlsm file upload is allowed",
			"error":       "only csv, xlsx, and xlsm file upload is allowed",
		})
	}

	uploaded, err := uploadFile.Open()
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "failed to read uploaded file",
			"error":       "failed to read uploaded file",
		})
	}
	defer uploaded.Close()

	tempFile, err := os.CreateTemp("", "global-instruments-*"+ext)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to create temporary file for upload",
			"error":       "failed to create temporary file for upload",
		})
	}

	if _, err := io.Copy(tempFile, uploaded); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to save uploaded file",
			"error":       "failed to save uploaded file",
		})
	}

	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempFile.Name())
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to finalize uploaded file",
			"error":       "failed to finalize uploaded file",
		})
	}

	filePath := tempFile.Name()
	defer os.Remove(filePath)

	result, err := models.ImportGlobalInstrumentsFromFile(ctl.DB, filePath)
	if err != nil {
		log.Printf("error importing global instruments from file %s: %v", filePath, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to import global instruments",
			"error":       err.Error(),
		})
	}

	if err := ctl.refreshFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "global instruments imported but feed sync failed",
			"error":       "global instruments imported but feed sync failed",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "global instruments imported successfully",
		"file_name":   uploadFile.Filename,
		"result":      result,
	})
}

// Create
func (ctl *AdminGlobalInstrumentController) Create(c *fiber.Ctx) error {
	var req globalInstrumentUpsertRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request payload",
			"error":       "invalid request payload",
		})
	}

	expiry, err := parseGlobalInstrumentExpiry(req.Expiry)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "expiry must be in dd-mm-yy format",
			"error":       "expiry must be in dd-mm-yy format",
		})
	}

	instrument := models.GlobalInstrument{
		InstrumentToken:     req.InstrumentToken,
		ExchangeToken:       req.ExchangeToken,
		TradingSymbol:       req.TradingSymbol,
		Name:                req.Name,
		SubscribeSymbolName: req.SubscribeSymbolName,
		Symbol:              req.Symbol,
		LastPrice:           req.LastPrice,
		Expiry:              expiry,
		TickSize:            req.TickSize,
		LotSize:             req.LotSize,
		InstrumentType:      req.InstrumentType,
		Segment:             req.Segment,
		Exchange:            req.Exchange,
		Strike:              req.Strike,
		Status:              req.Status,
		IsDeleted:           req.IsDeleted,
	}

	if err := validator.ValidateGlobalInstrument(&instrument); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     err.Error(),
			"error":       err.Error(),
		})
	}
	if err := ctl.DB.Create(&instrument).Error; err != nil {
		statusCode := fiber.StatusInternalServerError
		message := "failed to create global instrument"
		if errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			statusCode = fiber.StatusConflict
			message = "global instrument already exists"
		}
		return c.Status(statusCode).JSON(fiber.Map{
			"status_code": statusCode,
			"message":     message,
			"error":       message,
		})
	}
	if err := ctl.refreshFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "global instrument created but feed sync failed",
			"error":       "global instrument created but feed sync failed",
		})
	}
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code":       fiber.StatusCreated,
		"message":           "global instrument created successfully",
		"global_instrument": instrument,
	})
}

// Update (also handles soft delete/undelete)
func (ctl *AdminGlobalInstrumentController) Update(c *fiber.Ctx) error {
	id := c.Params("id")
	var req globalInstrumentUpsertRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request payload",
			"error":       "invalid request payload",
		})
	}

	expiry, err := parseGlobalInstrumentExpiry(req.Expiry)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "expiry must be in dd-mm-yy format",
			"error":       "expiry must be in dd-mm-yy format",
		})
	}

	incoming := models.GlobalInstrument{
		InstrumentToken:     req.InstrumentToken,
		ExchangeToken:       req.ExchangeToken,
		TradingSymbol:       req.TradingSymbol,
		Name:                req.Name,
		SubscribeSymbolName: req.SubscribeSymbolName,
		Symbol:              req.Symbol,
		LastPrice:           req.LastPrice,
		Expiry:              expiry,
		TickSize:            req.TickSize,
		LotSize:             req.LotSize,
		InstrumentType:      req.InstrumentType,
		Segment:             req.Segment,
		Exchange:            req.Exchange,
		Strike:              req.Strike,
		Status:              req.Status,
		IsDeleted:           req.IsDeleted,
	}

	if err := validator.ValidateGlobalInstrument(&incoming); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     err.Error(),
			"error":       err.Error(),
		})
	}

	var instrument models.GlobalInstrument
	if err := ctl.DB.First(&instrument, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "global instrument not found",
				"error":       "global instrument not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch global instrument",
			"error":       "failed to fetch global instrument",
		})
	}
	instrument.Name = incoming.Name
	instrument.InstrumentToken = incoming.InstrumentToken
	instrument.ExchangeToken = incoming.ExchangeToken
	instrument.TradingSymbol = incoming.TradingSymbol
	instrument.SubscribeSymbolName = incoming.SubscribeSymbolName
	instrument.LastPrice = incoming.LastPrice
	instrument.Expiry = incoming.Expiry
	instrument.TickSize = incoming.TickSize
	instrument.LotSize = incoming.LotSize
	instrument.InstrumentType = incoming.InstrumentType
	instrument.Segment = incoming.Segment
	instrument.Exchange = incoming.Exchange
	instrument.Strike = incoming.Strike
	instrument.Symbol = incoming.Symbol
	instrument.Status = incoming.Status
	// Soft delete/undelete logic
	instrument.IsDeleted = incoming.IsDeleted
	if err := ctl.DB.Save(&instrument).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update global instrument",
			"error":       "failed to update global instrument",
		})
	}
	if err := ctl.refreshFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "global instrument updated but feed sync failed",
			"error":       "global instrument updated but feed sync failed",
		})
	}
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code":       fiber.StatusOK,
		"message":           "global instrument updated successfully",
		"global_instrument": instrument,
	})
}
