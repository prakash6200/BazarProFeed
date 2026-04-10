package controller

import (
	"errors"
	"feedprovider/config"
	"feedprovider/middleware"
	"feedprovider/models"
	"feedprovider/validator"
	"log"
	"strings"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type UserController struct {
	db        *gorm.DB
	socketHub *config.SocketHub
}

type GlobalCandlesRequest struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	Page        int    `json:"page"`
	SizePerPage int    `json:"sizePerPage"`
}

type GlobalRawTicksRequest struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	Page        int    `json:"page"`
	SizePerPage int    `json:"sizePerPage"`
}

type ZerodhaCandlesRequest struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	Page        int    `json:"page"`
	SizePerPage int    `json:"sizePerPage"`
}

type ZerodhaRawTicksRequest struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	Page        int    `json:"page"`
	SizePerPage int    `json:"sizePerPage"`
}

func NewUserController(db *gorm.DB, socketHub *config.SocketHub) *UserController {
	return &UserController{
		db:        db,
		socketHub: socketHub,
	}
}

func (uc *UserController) Signup(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.SignupRequest)

	if _, err := models.GetUserByUsername(uc.db, req.Username); err == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"message":     "username already exists",
			"error":       "username already exists",
		})
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("error checking existing username: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to validate username",
			"error":       "failed to validate username",
		})
	}

	user, err := models.CreateUserWithPassword(uc.db, req.Username, req.Password, models.RoleUser)
	if err != nil {
		log.Printf("error creating user signup: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to signup",
			"error":       "failed to signup",
		})
	}

	jwtToken, err := middleware.GenerateJWT(user)
	if err != nil {
		log.Printf("error generating jwt for signup user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to generate jwt token",
			"error":       "failed to generate jwt token",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code": fiber.StatusCreated,
		"message":     "signup successful",
		"user": fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"jwt_token":          jwtToken,
			"api_token":          user.APIToken,
			"token_generated_at": user.TokenGeneratedAt,
		},
	})
}

func (uc *UserController) Login(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.LoginRequest)

	user, err := models.GetUserByUsername(uc.db, req.Username)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "invalid credentials",
				"error":       "invalid credentials",
			})
		}
		log.Printf("error fetching user by username: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "login failed",
			"error":       "login failed",
		})
	}

	if user.PasswordHash == "" || !models.CheckPasswordHash(req.Password, user.PasswordHash) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid credentials",
			"error":       "invalid credentials",
		})
	}

	if !user.IsActive {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"message":     "user account is disabled",
			"error":       "user account is disabled",
		})
	}

	if err := user.RefreshToken(uc.db); err != nil {
		log.Printf("error refreshing token on login for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to issue login token",
			"error":       "failed to issue login token",
		})
	}

	jwtToken, err := middleware.GenerateJWT(user)
	if err != nil {
		log.Printf("error generating jwt for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to generate jwt token",
			"error":       "failed to generate jwt token",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "login successful",
		"token": fiber.Map{
			"jwt_token":          jwtToken,
			"api_token":          user.APIToken,
			"token_generated_at": user.TokenGeneratedAt,
		},
		"user": fiber.Map{
			"id":         user.ID,
			"username":   user.Username,
			"role":       user.EffectiveRole(),
			"is_active":  user.IsActive,
			"created_at": user.CreatedAt,
			"updated_at": user.UpdatedAt,
		},
	})
}

func (uc *UserController) GetProfile(c *fiber.Ctx) error {
	user := c.Locals("user").(*models.User)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "profile fetched successfully",
		"user": fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"token_generated_at": user.TokenGeneratedAt,
			"is_active":          user.IsActive,
			"created_at":         user.CreatedAt,
		},
	})
}

func (uc *UserController) RefreshToken(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.RefreshTokenRequest)
	user := c.Locals("user").(*models.User)

	if req.OldToken != user.APIToken {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid token",
			"error":       "invalid token",
		})
	}

	if !user.IsActive {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"message":     "user account is disabled",
			"error":       "user account is disabled",
		})
	}

	uc.socketHub.CloseUserConnections(user.ID)

	if err := user.RefreshToken(uc.db); err != nil {
		log.Printf("error refreshing token for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to generate new token",
			"error":       "failed to generate new token",
		})
	}

	log.Printf("token refreshed for user: username=%s, id=%s", user.Username, user.ID)

	jwtToken, err := middleware.GenerateJWT(user)
	if err != nil {
		log.Printf("error generating jwt for refreshed token user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to generate jwt token",
			"error":       "failed to generate jwt token",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "token refreshed successfully",
		"token": fiber.Map{
			"jwt_token":          jwtToken,
			"api_token":          user.APIToken,
			"token_generated_at": user.TokenGeneratedAt,
		},
	})
}

func (uc *UserController) GetGlobalCandles(c *fiber.Ctx) error {
	var req GlobalCandlesRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	interval := strings.ToLower(strings.TrimSpace(req.Interval))
	intervalMinutes := 1
	switch interval {
	case "", "1m":
		interval = "1m"
		intervalMinutes = 1
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 5m, 15m",
			"error":       "interval must be one of: 1m, 5m, 15m",
		})
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.SizePerPage <= 0 {
		req.SizePerPage = 20
	}
	if req.SizePerPage > 200 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be less than or equal to 200",
			"error":       "sizePerPage must be less than or equal to 200",
		})
	}

	rows, total, err := models.ListGlobalCandlesLast24h(uc.db, intervalMinutes, req.Page, req.SizePerPage, models.GlobalTickQueryFilters{
		Exchange: req.Exchange,
		Symbol:   req.Symbol,
	})
	if err != nil {
		log.Printf("error fetching global candles: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch global candles",
			"error":       "failed to fetch global candles",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(req.SizePerPage) - 1) / int64(req.SizePerPage))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "global candles fetched successfully",
		"candles":     rows,
		"pagination": fiber.Map{
			"page":         req.Page,
			"sizePerPage":  req.SizePerPage,
			"totalRecords": total,
			"totalPages":   totalPages,
			"interval":     interval,
			"duration":     "24h",
		},
	})
}

func (uc *UserController) GetGlobalRawTicks(c *fiber.Ctx) error {
	var req GlobalRawTicksRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	interval := strings.ToLower(strings.TrimSpace(req.Interval))
	intervalMinutes := 1
	switch interval {
	case "", "1m":
		interval = "1m"
		intervalMinutes = 1
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 5m, 15m",
			"error":       "interval must be one of: 1m, 5m, 15m",
		})
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.SizePerPage <= 0 {
		req.SizePerPage = 50
	}
	if req.SizePerPage > 500 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be less than or equal to 500",
			"error":       "sizePerPage must be less than or equal to 500",
		})
	}

	candleTicks, total, err := models.ListGlobalCandleTicksLast24h(uc.db, intervalMinutes, req.Page, req.SizePerPage, models.GlobalTickQueryFilters{
		Exchange: req.Exchange,
		Symbol:   req.Symbol,
	})
	if err != nil {
		log.Printf("error fetching global candle ticks: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch global candle ticks",
			"error":       "failed to fetch global candle ticks",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(req.SizePerPage) - 1) / int64(req.SizePerPage))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code":  fiber.StatusOK,
		"message":      "global candle ticks fetched successfully",
		"candle_ticks": candleTicks,
		"pagination": fiber.Map{
			"page":         req.Page,
			"sizePerPage":  req.SizePerPage,
			"totalRecords": total,
			"totalPages":   totalPages,
			"interval":     interval,
			"duration":     "24h",
		},
	})
}

func (uc *UserController) GetZerodhaCandles(c *fiber.Ctx) error {
	var req ZerodhaCandlesRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	interval := strings.ToLower(strings.TrimSpace(req.Interval))
	intervalMinutes := 1
	switch interval {
	case "", "1m":
		interval = "1m"
		intervalMinutes = 1
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 5m, 15m",
			"error":       "interval must be one of: 1m, 5m, 15m",
		})
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.SizePerPage <= 0 {
		req.SizePerPage = 20
	}
	if req.SizePerPage > 200 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be less than or equal to 200",
			"error":       "sizePerPage must be less than or equal to 200",
		})
	}

	rows, total, err := models.ListZerodhaCandlesLast24h(uc.db, intervalMinutes, req.Page, req.SizePerPage, models.ZerodhaTickQueryFilters{
		Exchange: req.Exchange,
		Symbol:   req.Symbol,
	})
	if err != nil {
		log.Printf("error fetching zerodha candles: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch zerodha candles",
			"error":       "failed to fetch zerodha candles",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(req.SizePerPage) - 1) / int64(req.SizePerPage))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "zerodha candles fetched successfully",
		"candles":     rows,
		"pagination": fiber.Map{
			"page":         req.Page,
			"sizePerPage":  req.SizePerPage,
			"totalRecords": total,
			"totalPages":   totalPages,
			"interval":     interval,
			"duration":     "24h",
		},
	})
}

func (uc *UserController) GetZerodhaRawTicks(c *fiber.Ctx) error {
	var req ZerodhaRawTicksRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	interval := strings.ToLower(strings.TrimSpace(req.Interval))
	intervalMinutes := 1
	switch interval {
	case "", "1m":
		interval = "1m"
		intervalMinutes = 1
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 5m, 15m",
			"error":       "interval must be one of: 1m, 5m, 15m",
		})
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.SizePerPage <= 0 {
		req.SizePerPage = 50
	}
	if req.SizePerPage > 500 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "sizePerPage must be less than or equal to 500",
			"error":       "sizePerPage must be less than or equal to 500",
		})
	}

	candleTicks, total, err := models.ListZerodhaCandleTicksLast24h(uc.db, intervalMinutes, req.Page, req.SizePerPage, models.ZerodhaTickQueryFilters{
		Exchange: req.Exchange,
		Symbol:   req.Symbol,
	})
	if err != nil {
		log.Printf("error fetching zerodha candle ticks: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch zerodha candle ticks",
			"error":       "failed to fetch zerodha candle ticks",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(req.SizePerPage) - 1) / int64(req.SizePerPage))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code":  fiber.StatusOK,
		"message":      "zerodha candle ticks fetched successfully",
		"candle_ticks": candleTicks,
		"pagination": fiber.Map{
			"page":         req.Page,
			"sizePerPage":  req.SizePerPage,
			"totalRecords": total,
			"totalPages":   totalPages,
			"interval":     interval,
			"duration":     "24h",
		},
	})
}
