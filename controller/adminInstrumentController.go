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
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type AdminInstrumentController struct {
	db          *gorm.DB
	zerodhaFeed *services.ZerodhaFeedService
}

func NewAdminInstrumentController(db *gorm.DB, zerodhaFeed *services.ZerodhaFeedService) *AdminInstrumentController {
	return &AdminInstrumentController{db: db, zerodhaFeed: zerodhaFeed}
}

func (ic *AdminInstrumentController) refreshZerodhaFeed() error {
	if ic.zerodhaFeed == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return ic.zerodhaFeed.RefreshFromDatabase(ctx)
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
	uploadFile, err := c.FormFile("file")
	if err != nil || uploadFile == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "file is required in form-data",
			"error":       "file is required in form-data",
		})
	}

	if strings.ToLower(filepath.Ext(strings.TrimSpace(uploadFile.Filename))) != ".csv" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "only csv file upload is allowed",
			"error":       "only csv file upload is allowed",
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

	tempFile, err := os.CreateTemp("", "instruments-*.csv")
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

	csvPath := tempFile.Name()
	defer os.Remove(csvPath)

	result, err := models.ImportInstrumentsFromCSV(ic.db, csvPath)
	if err != nil {
		log.Printf("error importing instruments from csv %s: %v", csvPath, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to import instruments",
			"error":       err.Error(),
		})
	}

	if err := ic.refreshZerodhaFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "instruments imported but zerodha feed sync failed",
			"error":       "instruments imported but zerodha feed sync failed",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instruments imported successfully",
		"file_name":   uploadFile.Filename,
		"result":      result,
	})
}

func (ic *AdminInstrumentController) GetInstruments(c *fiber.Ctx) error {
	query := c.Locals("validated_instrument_list_query").(validator.InstrumentListQueryRequest)
	page := query.Page
	limit := query.Limit
	includeDeleted := query.IncludeDeleted
	filters := models.InstrumentListFilters{
		InstrumentType: query.InstrumentType,
		Segment:        query.Segment,
		Exchange:       query.Exchange,
		Status:         query.Status,
		ExpiryDate:     query.ExpiryDate,
		Search:         query.Search,
		SortBy:         query.SortBy,
		SortOrder:      query.SortOrder,
	}

	instruments, total, err := models.GetInstrumentsPaginated(ic.db, page, limit, includeDeleted, filters)
	if err != nil {
		log.Printf("error fetching paginated instruments: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch instruments",
			"error":       "failed to fetch instruments",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(limit) - 1) / int64(limit))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instruments fetched successfully",
		"instruments": instruments,
		"pagination": fiber.Map{
			"page":            page,
			"limit":           limit,
			"total_records":   total,
			"total_pages":     totalPages,
			"include_deleted": includeDeleted,
			"filters": fiber.Map{
				"instrument_type": filters.InstrumentType,
				"segment":         filters.Segment,
				"exchange":        filters.Exchange,
				"status":          filters.Status,
				"expiry":          filters.ExpiryDate,
				"search":          filters.Search,
				"sort_by":         filters.SortBy,
				"sort_order":      filters.SortOrder,
			},
		},
	})
}

func (ic *AdminInstrumentController) GetInstrument(c *fiber.Ctx) error {
	instrumentID := c.Locals("validated_instrument_id").(validator.InstrumentIDRequest).ID

	instrument, err := models.GetInstrumentByID(ic.db, instrumentID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "instrument not found",
				"error":       "instrument not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch instrument",
			"error":       "failed to fetch instrument",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instrument fetched successfully",
		"instrument":  instrument,
	})
}

func (ic *AdminInstrumentController) CreateInstrument(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.CreateInstrumentRequest)

	expiry, err := parseInstrumentExpiry(req.Expiry)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "expiry must be in dd-mm-yy format",
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
			"message":     "failed to create instrument",
			"error":       "failed to create instrument",
		})
	}

	if err := ic.refreshZerodhaFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "instrument created but zerodha feed sync failed",
			"error":       "instrument created but zerodha feed sync failed",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code": fiber.StatusCreated,
		"message":     "instrument created successfully",
		"instrument":  instrument,
	})
}

func (ic *AdminInstrumentController) UpdateInstrument(c *fiber.Ctx) error {
	instrumentID := c.Locals("validated_instrument_id").(validator.InstrumentIDRequest).ID
	req := c.Locals("validated_request").(validator.UpdateInstrumentRequest)

	if _, err := models.GetInstrumentByID(ic.db, instrumentID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "instrument not found",
				"error":       "instrument not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch instrument",
			"error":       "failed to fetch instrument",
		})
	}

	updates, err := buildInstrumentUpdateMap(req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "expiry must be in dd-mm-yy format",
			"error":       "expiry must be in dd-mm-yy format",
		})
	}

	if err := models.UpdateInstrumentFields(ic.db, instrumentID, updates); err != nil {
		log.Printf("error updating instrument %s: %v", instrumentID, err)
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"message":     "failed to update instrument",
			"error":       "failed to update instrument",
		})
	}

	if err := ic.refreshZerodhaFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "instrument updated but zerodha feed sync failed",
			"error":       "instrument updated but zerodha feed sync failed",
		})
	}

	instrument, err := models.GetInstrumentByID(ic.db, instrumentID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch updated instrument",
			"error":       "failed to fetch updated instrument",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instrument updated successfully",
		"instrument":  instrument,
	})
}

func buildInstrumentUpdateMap(req validator.UpdateInstrumentRequest) (map[string]interface{}, error) {
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
			return nil, err
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
	return updates, nil
}

func (ic *AdminInstrumentController) BulkUpdateInstruments(c *fiber.Ctx) error {
	req := c.Locals("validated_bulk_update_request").(validator.BulkUpdateInstrumentsRequest)

	tx := ic.db.Begin()
	if tx.Error != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to start bulk update transaction",
			"error":       "failed to start bulk update transaction",
		})
	}

	updatedInstruments := make([]models.Instrument, 0, len(req.Updates))
	for i, item := range req.Updates {
		if _, err := models.GetInstrumentByID(tx, item.ID); err != nil {
			tx.Rollback()
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
					"status_code": fiber.StatusNotFound,
					"message":     "instrument not found",
					"error":       "updates[" + strconv.Itoa(i) + "] instrument not found",
					"id":          item.ID,
				})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status_code": fiber.StatusInternalServerError,
				"message":     "failed to fetch instrument",
				"error":       "failed to fetch instrument",
			})
		}

		updates, err := buildInstrumentUpdateMap(item.UpdateInstrumentRequest)
		if err != nil {
			tx.Rollback()
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "expiry must be in dd-mm-yy format",
				"error":       "updates[" + strconv.Itoa(i) + "].expiry must be in dd-mm-yy format",
			})
		}

		if err := models.UpdateInstrumentFields(tx, item.ID, updates); err != nil {
			tx.Rollback()
			log.Printf("error bulk updating instrument %s: %v", item.ID, err)
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"status_code": fiber.StatusConflict,
				"message":     "failed to bulk update instruments",
				"error":       "updates[" + strconv.Itoa(i) + "] failed to update",
				"id":          item.ID,
			})
		}

		updated, err := models.GetInstrumentByID(tx, item.ID)
		if err != nil {
			tx.Rollback()
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status_code": fiber.StatusInternalServerError,
				"message":     "failed to fetch updated instrument",
				"error":       "failed to fetch updated instrument",
			})
		}
		updatedInstruments = append(updatedInstruments, *updated)
	}

	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to commit bulk update",
			"error":       "failed to commit bulk update",
		})
	}

	if err := ic.refreshZerodhaFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "bulk update completed but zerodha feed sync failed",
			"error":       "bulk update completed but zerodha feed sync failed",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code":         fiber.StatusOK,
		"message":             "instruments bulk updated successfully",
		"updated_count":       len(updatedInstruments),
		"updated_instruments": updatedInstruments,
	})
}

func (ic *AdminInstrumentController) DeleteInstrument(c *fiber.Ctx) error {
	instrumentID := c.Locals("validated_instrument_id").(validator.InstrumentIDRequest).ID

	if _, err := models.GetInstrumentByID(ic.db, instrumentID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "instrument not found",
				"error":       "instrument not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch instrument",
			"error":       "failed to fetch instrument",
		})
	}

	if err := models.SoftDeleteInstrumentByID(ic.db, instrumentID); err != nil {
		log.Printf("error deleting instrument %s: %v", instrumentID, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to soft delete instrument",
			"error":       "failed to soft delete instrument",
		})
	}

	if err := ic.refreshZerodhaFeed(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "instrument deleted but zerodha feed sync failed",
			"error":       "instrument deleted but zerodha feed sync failed",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instrument soft deleted successfully",
	})
}
