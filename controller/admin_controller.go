package controller

import (
	"errors"
	"feedprovider/config"
	"feedprovider/models"
	"feedprovider/validator"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type AdminController struct {
	db        *gorm.DB
	socketHub *config.SocketHub
}

func NewAdminController(db *gorm.DB, socketHub *config.SocketHub) *AdminController {
	return &AdminController{
		db:        db,
		socketHub: socketHub,
	}
}

func (ac *AdminController) CreateUser(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.CreateUserRequest)

	var existingUser models.User
	if err := ac.db.Where("username = ?", req.Username).First(&existingUser).Error; err == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"error":       "username already exists",
		})
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("error checking existing username: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to validate username",
		})
	}

	user, err := models.CreateUser(ac.db, req.Username, false)
	if err != nil {
		log.Printf("error creating user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to create user",
		})
	}

	log.Printf("user created: username=%s, id=%s", user.Username, user.ID)

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code": fiber.StatusCreated,
		"message":     "user created successfully",
		"user": fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"api_token":          user.APIToken,
			"token_generated_at": user.TokenGeneratedAt,
			"is_active":          user.IsActive,
			"created_at":         user.CreatedAt,
		},
	})
}

func (ac *AdminController) GetAllUsers(c *fiber.Ctx) error {
	users, err := models.GetAllUsers(ac.db)
	if err != nil {
		log.Printf("error fetching users: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to fetch users",
		})
	}

	userList := make([]fiber.Map, len(users))
	for i, user := range users {
		userList[i] = fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"token_generated_at": user.TokenGeneratedAt,
			"is_active":          user.IsActive,
			"is_admin":           user.IsAdmin,
			"created_at":         user.CreatedAt,
			"updated_at":         user.UpdatedAt,
		}
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"users":       userList,
		"count":       len(users),
	})
}

func (ac *AdminController) GetUser(c *fiber.Ctx) error {
	userID := c.Params("id")

	user, err := models.GetUserByID(ac.db, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"error":       "user not found",
			})
		}
		log.Printf("error fetching user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to fetch user",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"user": fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"api_token":          user.APIToken,
			"token_generated_at": user.TokenGeneratedAt,
			"is_active":          user.IsActive,
			"is_admin":           user.IsAdmin,
			"created_at":         user.CreatedAt,
			"updated_at":         user.UpdatedAt,
		},
	})
}

func (ac *AdminController) UpdateUserStatus(c *fiber.Ctx) error {
	userID := c.Params("id")
	req := c.Locals("validated_request").(validator.UpdateStatusRequest)

	user, err := models.GetUserByID(ac.db, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"error":       "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to fetch user",
		})
	}

	if err := models.UpdateUserStatus(ac.db, userID, req.IsActive); err != nil {
		log.Printf("error updating user status: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to update user status",
		})
	}

	if !req.IsActive {
		ac.socketHub.CloseUserConnections(user.ID)
		log.Printf("user disabled and connections closed: username=%s, id=%s", user.Username, userID)
	}

	log.Printf("user status updated: username=%s, id=%s, is_active=%v", user.Username, userID, req.IsActive)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "user status updated successfully",
		"is_active":   req.IsActive,
	})
}

func (ac *AdminController) DeleteUser(c *fiber.Ctx) error {
	userID := c.Params("id")

	user, err := models.GetUserByID(ac.db, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"error":       "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to fetch user",
		})
	}

	if user.IsAdmin {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"error":       "cannot delete admin user",
		})
	}

	ac.socketHub.CloseUserConnections(user.ID)

	if err := models.DeleteUser(ac.db, userID); err != nil {
		log.Printf("error deleting user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to delete user",
		})
	}

	log.Printf("user deleted: username=%s, id=%s", user.Username, userID)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "user deleted successfully",
	})
}

func (ac *AdminController) GetStats(c *fiber.Ctx) error {

	var totalUsers int64
	ac.db.Model(&models.User{}).Count(&totalUsers)

	var activeUsers int64
	ac.db.Model(&models.User{}).Where("is_active = ?", true).Count(&activeUsers)

	connectedClients := ac.socketHub.GetClientCount()

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"stats": fiber.Map{
			"total_users":       totalUsers,
			"active_users":      activeUsers,
			"connected_clients": connectedClients,
		},
	})
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

func (ac *AdminController) ImportInstruments(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.ImportInstrumentsRequest)

	result, err := models.ImportInstrumentsFromCSV(ac.db, req.FilePath)
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

func (ac *AdminController) CreateInstrument(c *fiber.Ctx) error {
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
	}

	if err := models.CreateInstrument(ac.db, instrument); err != nil {
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

func (ac *AdminController) UpdateInstrument(c *fiber.Ctx) error {
	instrumentID := c.Params("id")
	req := c.Locals("validated_request").(validator.UpdateInstrumentRequest)

	if _, err := models.GetInstrumentByID(ac.db, instrumentID); err != nil {
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

	if err := models.UpdateInstrumentFields(ac.db, instrumentID, updates); err != nil {
		log.Printf("error updating instrument %s: %v", instrumentID, err)
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"error":       "failed to update instrument",
		})
	}

	instrument, err := models.GetInstrumentByID(ac.db, instrumentID)
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

func (ac *AdminController) DeleteInstrument(c *fiber.Ctx) error {
	instrumentID := c.Params("id")

	if _, err := models.GetInstrumentByID(ac.db, instrumentID); err != nil {
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

	if err := models.DeleteInstrumentByID(ac.db, instrumentID); err != nil {
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
