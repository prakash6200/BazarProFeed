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

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type AdminGlobalInstrumentController struct {
	DB      *gorm.DB
	FeedSvc services.MarketFeedProvider
}

func NewAdminGlobalInstrumentController(db *gorm.DB, feedSvc services.MarketFeedProvider) *AdminGlobalInstrumentController {
	return &AdminGlobalInstrumentController{DB: db, FeedSvc: feedSvc}
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
	var req models.GlobalInstrument
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request payload",
			"error":       "invalid request payload",
		})
	}
	if err := validator.ValidateGlobalInstrument(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     err.Error(),
			"error":       err.Error(),
		})
	}
	if err := ctl.DB.Create(&req).Error; err != nil {
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
		"global_instrument": req,
	})
}

// Update (also handles soft delete/undelete)
func (ctl *AdminGlobalInstrumentController) Update(c *fiber.Ctx) error {
	id := c.Params("id")
	var req models.GlobalInstrument
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request payload",
			"error":       "invalid request payload",
		})
	}
	if err := validator.ValidateGlobalInstrument(&req); err != nil {
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
	instrument.Name = req.Name
	instrument.InstrumentToken = req.InstrumentToken
	instrument.ExchangeToken = req.ExchangeToken
	instrument.TradingSymbol = req.TradingSymbol
	instrument.SubscribeSymbolName = req.SubscribeSymbolName
	instrument.LastPrice = req.LastPrice
	instrument.Expiry = req.Expiry
	instrument.TickSize = req.TickSize
	instrument.LotSize = req.LotSize
	instrument.InstrumentType = req.InstrumentType
	instrument.Segment = req.Segment
	instrument.Exchange = req.Exchange
	instrument.Strike = req.Strike
	instrument.Symbol = req.Symbol
	instrument.Status = req.Status
	// Soft delete/undelete logic
	instrument.IsDeleted = req.IsDeleted
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
