package controller

import (
	"errors"
	"feedprovider/models"
	"feedprovider/services"
	"feedprovider/validator"
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
	subscribeSymbol := strings.TrimSpace(req.SubscribeSymbolName)
	if subscribeSymbol == "" {
		subscribeSymbol = strings.TrimSpace(req.Symbol)
	}
	if req.IsActive() {
		ctl.FeedSvc.Subscribe([]string{subscribeSymbol})
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
	subscribeSymbol := strings.TrimSpace(instrument.SubscribeSymbolName)
	if subscribeSymbol == "" {
		subscribeSymbol = strings.TrimSpace(instrument.Symbol)
	}
	// Subscribe/unsubscribe only if not deleted
	if instrument.IsDeleted {
		ctl.FeedSvc.Unsubscribe([]string{subscribeSymbol})
	} else if instrument.IsActive() {
		ctl.FeedSvc.Subscribe([]string{subscribeSymbol})
	} else {
		ctl.FeedSvc.Unsubscribe([]string{subscribeSymbol})
	}
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code":       fiber.StatusOK,
		"message":           "global instrument updated successfully",
		"global_instrument": instrument,
	})
}
