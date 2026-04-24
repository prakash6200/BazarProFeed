package controller

import (
	"context"
	"errors"
	"feedprovider/config"
	"feedprovider/middleware"
	"feedprovider/models"
	"feedprovider/services"
	"feedprovider/validator"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// Soft request-level deadlines. Every DB call the user controller
// makes gets wrapped with one of these so a stalled analytics query
// (or a saturated pool) cannot pin a fasthttp worker forever. The
// hard cap is enforced server-side by the Postgres statement_timeout
// configured in config/database.go; these values simply fail the
// request a little earlier so the client gets a clean 503.
const (
	authDBTimeout      = 2 * time.Second
	instrumentsTimeout = 5 * time.Second
	analyticsTimeout   = 20 * time.Second
)

// scopedDB derives a bounded context from the in-flight request and
// returns the DB handle bound to it. If the client disconnects or
// the deadline fires, the underlying SQL driver will abort the query
// and release the Postgres connection back to the pool.
func scopedDB(c *fiber.Ctx, db *gorm.DB, d time.Duration) (*gorm.DB, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.UserContext(), d)
	return db.WithContext(ctx), cancel
}

// isDBBusyErr reports whether the error indicates the pool or query
// was bounded out (deadline, cancellation, or Postgres statement
// timeout). These should be surfaced as 503, not 500, because the
// correct client behaviour is to retry with backoff.
func isDBBusyErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "canceling statement due to statement timeout") ||
		strings.Contains(msg, "canceling statement due to user request")
}

type UserController struct {
	db          *gorm.DB
	socketHub   *config.SocketHub
	redisClient *redis.Client
}

type GlobalCandlesRequest struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	Page        int    `json:"page"`
	SizePerPage int    `json:"sizePerPage"`
}

type GlobalRawTicksRequest struct {
	Exchange      string `json:"exchange"`
	Symbol        string `json:"symbol"`
	Interval      string `json:"interval"`
	IntervalStart string `json:"interval_start"`
	Page          int    `json:"page"`
	SizePerPage   int    `json:"sizePerPage"`
}

type ZerodhaCandlesRequest struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	Page        int    `json:"page"`
	SizePerPage int    `json:"sizePerPage"`
}

type ZerodhaRawTicksRequest struct {
	Exchange      string `json:"exchange"`
	Symbol        string `json:"symbol"`
	Interval      string `json:"interval"`
	IntervalStart string `json:"interval_start"`
	Page          int    `json:"page"`
	SizePerPage   int    `json:"sizePerPage"`
}

func NewUserController(db *gorm.DB, socketHub *config.SocketHub, redisClient *redis.Client) *UserController {
	return &UserController{
		db:          db,
		socketHub:   socketHub,
		redisClient: redisClient,
	}
}

func (uc *UserController) Signup(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.SignupRequest)

	lookupDB, lookupCancel := scopedDB(c, uc.db, authDBTimeout)
	_, existsErr := models.GetUserByUsername(lookupDB, req.Username)
	lookupCancel()
	if existsErr == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"message":     "username already exists",
			"error":       "username already exists",
		})
	} else if isDBBusyErr(existsErr) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"status_code": fiber.StatusServiceUnavailable,
			"message":     "signup service busy",
			"error":       "signup service busy",
		})
	} else if !errors.Is(existsErr, gorm.ErrRecordNotFound) {
		log.Printf("error checking existing username: %v", existsErr)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to validate username",
			"error":       "failed to validate username",
		})
	}

	createDB, createCancel := scopedDB(c, uc.db, instrumentsTimeout)
	user, err := models.CreateUserWithPassword(createDB, req.Username, req.Password, models.RoleUser)
	createCancel()
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

	lookupDB, lookupCancel := scopedDB(c, uc.db, authDBTimeout)
	user, err := models.GetUserByUsername(lookupDB, req.Username)
	lookupCancel()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "invalid credentials",
				"error":       "invalid credentials",
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "login service busy",
				"error":       "login service busy",
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

	refreshDB, refreshCancel := scopedDB(c, uc.db, authDBTimeout)
	err = user.RefreshToken(refreshDB)
	refreshCancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "login service busy",
				"error":       "login service busy",
			})
		}
		log.Printf("error refreshing token on login for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to issue login token",
			"error":       "failed to issue login token",
		})
	}

	// Drop any stale cached copy of this user so the fresh
	// TokenGeneratedAt/APIToken take effect immediately on the
	// first authenticated request after login.
	middleware.InvalidateUserAuthCache(user.ID)

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

	refreshDB, refreshCancel := scopedDB(c, uc.db, authDBTimeout)
	err := user.RefreshToken(refreshDB)
	refreshCancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "token service busy",
				"error":       "token service busy",
			})
		}
		log.Printf("error refreshing token for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to generate new token",
			"error":       "failed to generate new token",
		})
	}

	middleware.InvalidateUserAuthCache(user.ID)
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

func (uc *UserController) ChangePassword(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.AdminChangePasswordRequest)

	user, ok := c.Locals("user").(*models.User)
	if !ok || user == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid authenticated user",
			"error":       "invalid authenticated user",
		})
	}

	if user.PasswordHash == "" || !models.CheckPasswordHash(req.CurrentPassword, user.PasswordHash) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid current_password",
			"error":       "invalid current_password",
		})
	}

	newPasswordHash, err := models.HashPassword(req.NewPassword)
	if err != nil {
		log.Printf("error hashing new password for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update password",
			"error":       "failed to update password",
		})
	}

	txDB, txCancel := scopedDB(c, uc.db, instrumentsTimeout)
	err = txDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.User{}).Where("id = ?", user.ID).Update("password_hash", newPasswordHash).Error; err != nil {
			return err
		}

		if err := user.RefreshToken(tx); err != nil {
			return err
		}

		return nil
	})
	txCancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "password service busy",
				"error":       "password service busy",
			})
		}
		log.Printf("error changing password for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to change password",
			"error":       "failed to change password",
		})
	}

	uc.socketHub.CloseUserConnections(user.ID)
	middleware.InvalidateUserAuthCache(user.ID)
	log.Printf("user password changed: username=%s, id=%s", user.Username, user.ID)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "password changed successfully, please login again",
	})
}

func (uc *UserController) GetZerodhaInstruments(c *fiber.Ctx) error {
	query := c.Locals("validated_instrument_list_query").(validator.InstrumentListQueryRequest)
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

	db, cancel := scopedDB(c, uc.db, instrumentsTimeout)
	instruments, total, err := models.GetInstrumentsPaginated(db, query.Page, query.Limit, false, filters)
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "instruments service busy",
				"error":       "instruments service busy",
			})
		}
		log.Printf("error fetching paginated instruments for user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch instruments",
			"error":       "failed to fetch instruments",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(query.Limit) - 1) / int64(query.Limit))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "instruments fetched successfully",
		"instruments": instruments,
		"pagination": fiber.Map{
			"page":          query.Page,
			"limit":         query.Limit,
			"total_records": total,
			"total_pages":   totalPages,
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

func (uc *UserController) GetGlobalInstruments(c *fiber.Ctx) error {
	query := c.Locals("validated_global_instrument_list_query").(validator.GlobalInstrumentListQueryRequest)
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

	db, cancel := scopedDB(c, uc.db, instrumentsTimeout)
	instruments, total, err := models.GetGlobalInstrumentsPaginated(db, query.Page, query.Limit, false, filters)
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "instruments service busy",
				"error":       "instruments service busy",
			})
		}
		log.Printf("error fetching paginated global instruments for user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch global instruments",
			"error":       "failed to fetch global instruments",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(query.Limit) - 1) / int64(query.Limit))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "global instruments fetched successfully",
		"instruments": instruments,
		"pagination": fiber.Map{
			"page":          query.Page,
			"limit":         query.Limit,
			"total_records": total,
			"total_pages":   totalPages,
			"filters": fiber.Map{
				"status":          filters.Status,
				"segment":         filters.Segment,
				"exchange":        filters.Exchange,
				"instrument_type": filters.InstrumentType,
				"expiry":          filters.Expiry,
				"search":          filters.Search,
				"sort_by":         filters.SortBy,
				"sort_order":      filters.SortOrder,
			},
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
	case "3m":
		intervalMinutes = 3
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	case "30m":
		intervalMinutes = 30
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 3m, 5m, 15m, 30m",
			"error":       "interval must be one of: 1m, 3m, 5m, 15m, 30m",
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

	req.Symbol = strings.TrimSpace(req.Symbol)
	if req.Symbol == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "symbol is required",
			"error":       "symbol is required",
		})
	}

	// Try Redis first — CandleBuilder continuously pre-builds these in the background.
	// Postgres is only hit on a cold start / Redis miss.
	now := time.Now().UTC()
	windowStart := now.Add(-24 * time.Hour)
	if bars, err := services.GetCandlesFromRedis(
		c.UserContext(), uc.redisClient, "global",
		req.Symbol, intervalMinutes, windowStart, now,
	); err == nil && len(bars) > 0 {
		// Apply pagination over the Redis result set (newest first).
		total := int64(len(bars))
		// bars from GetCandlesFromRedis are oldest-first; reverse for newest-first.
		for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
			bars[i], bars[j] = bars[j], bars[i]
		}
		offset := (req.Page - 1) * req.SizePerPage
		if offset > len(bars) {
			offset = len(bars)
		}
		end := offset + req.SizePerPage
		if end > len(bars) {
			end = len(bars)
		}
		page := bars[offset:end]
		totalPages := int((total + int64(req.SizePerPage) - 1) / int64(req.SizePerPage))
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status_code": fiber.StatusOK,
			"message":     "global candles fetched successfully",
			"candles":     page,
			"source":      "cache",
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

	// Redis miss — fall back to Postgres.
	db, cancel := scopedDB(c, uc.db, analyticsTimeout)
	rows, total, err := models.ListGlobalCandlesLast24h(db, intervalMinutes, req.Page, req.SizePerPage, models.GlobalTickQueryFilters{
		Symbol: req.Symbol,
	})
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "analytics query timed out; please retry",
				"error":       "analytics query timed out",
			})
		}
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
		"source":      "db",
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
	case "3m":
		intervalMinutes = 3
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	case "30m":
		intervalMinutes = 30
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 3m, 5m, 15m, 30m",
			"error":       "interval must be one of: 1m, 3m, 5m, 15m, 30m",
		})
	}

	req.Symbol = strings.TrimSpace(req.Symbol)
	req.IntervalStart = strings.TrimSpace(req.IntervalStart)

	if req.Symbol == "" || req.IntervalStart == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "symbol and interval_start are required to fetch one candle ticks",
			"error":       "symbol and interval_start are required to fetch one candle ticks",
		})
	}

	// Always fetch exactly one candle bucket for this API.
	req.Page = 1
	req.SizePerPage = 1

	var intervalStartPtr *time.Time
	var intervalEndPtr *time.Time
	parsed, err := time.Parse(time.RFC3339, req.IntervalStart)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval_start must be RFC3339 format",
			"error":       "interval_start must be RFC3339 format",
		})
	}
	start := parsed.UTC()
	end := start.Add(time.Duration(intervalMinutes) * time.Minute)
	intervalStartPtr = &start
	intervalEndPtr = &end

	db, cancel := scopedDB(c, uc.db, analyticsTimeout)
	candleTicks, total, err := models.ListGlobalCandleTicksLast24h(db, intervalMinutes, req.Page, req.SizePerPage, models.GlobalTickQueryFilters{
		Symbol:        req.Symbol,
		IntervalStart: intervalStartPtr,
		IntervalEnd:   intervalEndPtr,
	})
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "analytics query timed out; please retry",
				"error":       "analytics query timed out",
			})
		}
		log.Printf("error fetching global candle ticks: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch global candle ticks",
			"error":       "failed to fetch global candle ticks",
		})
	}

	var candleTick interface{}
	if len(candleTicks) > 0 {
		candleTick = candleTicks[0]
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "global selected candle ticks fetched successfully",
		"candle_tick": candleTick,
		"meta": fiber.Map{
			"totalRecords": total,
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
	case "3m":
		intervalMinutes = 3
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	case "30m":
		intervalMinutes = 30
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 3m, 5m, 15m, 30m",
			"error":       "interval must be one of: 1m, 3m, 5m, 15m, 30m",
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

	req.Symbol = strings.TrimSpace(req.Symbol)
	if req.Symbol == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "symbol is required",
			"error":       "symbol is required",
		})
	}

	// Try Redis first — CandleBuilder continuously pre-builds these in the background.
	// Postgres is only hit on a cold start / Redis miss.
	now := time.Now().UTC()
	windowStart := now.Add(-24 * time.Hour)
	if bars, err := services.GetCandlesFromRedis(
		c.UserContext(), uc.redisClient, "zerodha",
		req.Symbol, intervalMinutes, windowStart, now,
	); err == nil && len(bars) > 0 {
		total := int64(len(bars))
		// bars from GetCandlesFromRedis are oldest-first; reverse for newest-first.
		for i, j := 0, len(bars)-1; i < j; i, j = i+1, j-1 {
			bars[i], bars[j] = bars[j], bars[i]
		}
		offset := (req.Page - 1) * req.SizePerPage
		if offset > len(bars) {
			offset = len(bars)
		}
		end := offset + req.SizePerPage
		if end > len(bars) {
			end = len(bars)
		}
		page := bars[offset:end]
		totalPages := int((total + int64(req.SizePerPage) - 1) / int64(req.SizePerPage))
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status_code": fiber.StatusOK,
			"message":     "zerodha candles fetched successfully",
			"candles":     page,
			"source":      "cache",
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

	// Redis miss — fall back to Postgres.
	db, cancel := scopedDB(c, uc.db, analyticsTimeout)
	rows, total, err := models.ListZerodhaCandlesLast24h(db, intervalMinutes, req.Page, req.SizePerPage, models.ZerodhaTickQueryFilters{
		Symbol: req.Symbol,
	})
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "analytics query timed out; please retry",
				"error":       "analytics query timed out",
			})
		}
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
		"source":      "db",
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
	case "3m":
		intervalMinutes = 3
	case "5m":
		intervalMinutes = 5
	case "15m":
		intervalMinutes = 15
	case "30m":
		intervalMinutes = 30
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "interval must be one of: 1m, 3m, 5m, 15m, 30m",
			"error":       "interval must be one of: 1m, 3m, 5m, 15m, 30m",
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

	req.Symbol = strings.TrimSpace(req.Symbol)
	if req.Symbol == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "symbol is required",
			"error":       "symbol is required",
		})
	}

	var intervalStartPtr *time.Time
	var intervalEndPtr *time.Time
	if strings.TrimSpace(req.IntervalStart) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(req.IntervalStart))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "interval_start must be RFC3339 format",
				"error":       "interval_start must be RFC3339 format",
			})
		}
		start := parsed.UTC()
		end := start.Add(time.Duration(intervalMinutes) * time.Minute)
		intervalStartPtr = &start
		intervalEndPtr = &end
	}

	db, cancel := scopedDB(c, uc.db, analyticsTimeout)
	candleTicks, total, err := models.ListZerodhaCandleTicksLast24h(db, intervalMinutes, req.Page, req.SizePerPage, models.ZerodhaTickQueryFilters{
		Symbol:        req.Symbol,
		IntervalStart: intervalStartPtr,
		IntervalEnd:   intervalEndPtr,
	})
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status_code": fiber.StatusServiceUnavailable,
				"message":     "analytics query timed out; please retry",
				"error":       "analytics query timed out",
			})
		}
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
