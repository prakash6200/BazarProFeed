package controller

import (
	"errors"
	"feedprovider/models"
	"feedprovider/validator"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type AdminInstrumentController struct {
	db *gorm.DB
}

func NewAdminInstrumentController(db *gorm.DB) *AdminInstrumentController {
	return &AdminInstrumentController{db: db}
}

func parseInstrumentExpiry(value string) (*time.Time, error) {
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

func (ic *AdminInstrumentController) ImportInstruments(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.ImportInstrumentsRequest)

	result, err := models.ImportInstrumentsFromCSV(ic.db, req.FilePath)
	if err != nil {
		log.Printf("error importing instruments from csv %s: %v", req.FilePath, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to import instruments",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instruments imported successfully",
		"file_path":   req.FilePath,
		"result":      result,
	})
}

func (ic *AdminInstrumentController) CreateInstrument(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.CreateInstrumentRequest)

	expiry, err := parseInstrumentExpiry(req.Expiry)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"error":       "expiry must be in dd-mm-yy format",
		})
	}

	instrument := &models.Instrument{
		InstrumentToken: req.InstrumentToken,
		ExchangeToken:   req.ExchangeToken,
		TradingSymbol:   req.TradingSymbol,
		Name:            req.Name,
		LastPrice:       req.LastPrice,
		Expiry:          expiry,
		Strike:          req.Strike,
		TickSize:        req.TickSize,
		LotSize:         req.LotSize,
		InstrumentType:  req.InstrumentType,
		Segment:         req.Segment,
		Exchange:        req.Exchange,
		Status:          models.NormalizeInstrumentStatus(req.Status),
	}

	if err := models.CreateInstrument(ic.db, instrument); err != nil {
		log.Printf("error creating instrument: %v", err)
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"error":       "failed to create instrument",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code": fiber.StatusCreated,
		"message":     "instrument created successfully",
		"instrument":  instrument,
	})
}

func (ic *AdminInstrumentController) UpdateInstrument(c *fiber.Ctx) error {
	instrumentID := c.Params("id")
	req := c.Locals("validated_request").(validator.UpdateInstrumentRequest)

	if _, err := models.GetInstrumentByID(ic.db, instrumentID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"error":       "instrument not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to fetch instrument",
		})
	}

	updates := make(map[string]interface{})
	if req.InstrumentToken != nil {
		updates["instrument_token"] = *req.InstrumentToken
	}
	if req.ExchangeToken != nil {
		updates["exchange_token"] = *req.ExchangeToken
	}
	if req.TradingSymbol != nil {
		updates["trading_symbol"] = *req.TradingSymbol
	}
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.LastPrice != nil {
		updates["last_price"] = *req.LastPrice
	}
	if req.Expiry != nil {
		expiry, err := parseInstrumentExpiry(*req.Expiry)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"error":       "expiry must be in dd-mm-yy format",
			})
		}
		updates["expiry"] = expiry
	}
	if req.Strike != nil {
		updates["strike"] = *req.Strike
	}
	if req.TickSize != nil {
		updates["tick_size"] = *req.TickSize
	}
	if req.LotSize != nil {
		updates["lot_size"] = *req.LotSize
	}
	if req.InstrumentType != nil {
		updates["instrument_type"] = *req.InstrumentType
	}
	if req.Segment != nil {
		updates["segment"] = *req.Segment
	}
	if req.Exchange != nil {
		updates["exchange"] = *req.Exchange
	}
	if req.Status != nil {
		updates["status"] = models.NormalizeInstrumentStatus(*req.Status)
	}

	if err := models.UpdateInstrumentFields(ic.db, instrumentID, updates); err != nil {
		log.Printf("error updating instrument %s: %v", instrumentID, err)
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"error":       "failed to update instrument",
		})
	}

	instrument, err := models.GetInstrumentByID(ic.db, instrumentID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to fetch updated instrument",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instrument updated successfully",
		"instrument":  instrument,
	})
}

func (ic *AdminInstrumentController) DeleteInstrument(c *fiber.Ctx) error {
	instrumentID := c.Params("id")

	if _, err := models.GetInstrumentByID(ic.db, instrumentID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"error":       "instrument not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to fetch instrument",
		})
	}

	if err := models.DeleteInstrumentByID(ic.db, instrumentID); err != nil {
		log.Printf("error deleting instrument %s: %v", instrumentID, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to delete instrument",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instrument deleted successfully",
	})
}
