package controller

import (
	"feedprovider/models"
	"feedprovider/services"
	"feedprovider/validator"

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
		Status: query.Status,
		Search: query.Search,
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
				"status": query.Status,
				"search": query.Search,
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
			"message":     "invalid request body",
			"error":       "invalid request body",
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
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to create global instrument",
			"error":       err.Error(),
		})
	}
	if req.IsActive() {
		ctl.FeedSvc.Subscribe([]string{req.Symbol})
	}
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code": fiber.StatusCreated,
		"message":     "global instrument created successfully",
		"instrument":  req,
	})
}

// Update (also handles soft delete/undelete)
func (ctl *AdminGlobalInstrumentController) Update(c *fiber.Ctx) error {
	id := c.Params("id")
	var req models.GlobalInstrument
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
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
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"status_code": fiber.StatusNotFound,
			"message":     "global instrument not found",
			"error":       "global instrument not found",
		})
	}

	// Capture old state before update so we can handle symbol/status changes correctly.
	oldSymbol := instrument.Symbol
	wasSubscribed := instrument.IsActive() && !instrument.IsDeleted

	instrument.Name = req.Name
	instrument.Symbol = req.Symbol
	instrument.Status = req.Status
	// Soft delete/undelete logic
	instrument.IsDeleted = req.IsDeleted
	if err := ctl.DB.Save(&instrument).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update global instrument",
			"error":       err.Error(),
		})
	}

	nowActive := instrument.IsActive() && !instrument.IsDeleted

	// If the symbol was renamed, unsubscribe the old symbol first.
	if oldSymbol != instrument.Symbol && wasSubscribed {
		ctl.FeedSvc.Unsubscribe([]string{oldSymbol})
	}

	// Subscribe when ACTIVE, unsubscribe when INACTIVE or deleted.
	if nowActive {
		ctl.FeedSvc.Subscribe([]string{instrument.Symbol})
	} else {
		ctl.FeedSvc.Unsubscribe([]string{instrument.Symbol})
	}
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "global instrument updated successfully",
		"instrument":  instrument,
	})
}
