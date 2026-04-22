package controller

import (
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

// Admin controller deadlines. These are intentionally tight — under a
// saturated pool a 5s wait per request is already enough to wedge all
// fasthttp workers, so we want to fail fast with 503 and let the
// client back off. The hard cap is enforced server-side by the
// Postgres statement_timeout configured in config/database.go.
const (
	adminDBTimeout        = 3 * time.Second
	adminWriteTxTimeout   = 10 * time.Second
	adminImportTimeout    = 120 * time.Second
	adminAnalyticsTimeout = 20 * time.Second
)

// dbBusyJSON is the canonical 503 body we return when the DB is under
// pressure. Keeping it a single function makes it trivial to audit
// every 503 return path to a query deadline.
func dbBusyJSON() fiber.Map {
	return fiber.Map{
		"status_code": fiber.StatusServiceUnavailable,
		"message":     "database service busy, please retry",
		"error":       "database service busy",
	}
}

// parsePageLimit extracts and validates `page` and `limit` query
// parameters. `limit` is hard-capped at maxLimit so a hostile client
// can't ask for a million rows and wedge both the DB and the
// serializer. On validation failure it writes a 400 and returns the
// error so the caller can early-return.
func parsePageLimit(c *fiber.Ctx, defaultLimit, maxLimit int) (int, int, error) {
	page := 1
	if raw := strings.TrimSpace(c.Query("page")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return 0, 0, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "page must be a positive integer",
				"error":       "page must be a positive integer",
			})
		}
		page = parsed
	}

	limit := defaultLimit
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return 0, 0, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be a positive integer",
				"error":       "limit must be a positive integer",
			})
		}
		if parsed > maxLimit {
			return 0, 0, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be less than or equal to " + strconv.Itoa(maxLimit),
				"error":       "limit must be less than or equal to " + strconv.Itoa(maxLimit),
			})
		}
		limit = parsed
	}

	return page, limit, nil
}
