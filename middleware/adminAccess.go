package middleware

import (
	"strings"

	"feedprovider/models"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

const (
	PermUsersCreate             = "users:create"
	PermUsersRead               = "users:read"
	PermUsersUpdate             = "users:update"
	PermUsersDelete             = "users:delete"
	PermUsersChangePassword     = "users:change_password"
	PermUsersLogout             = "users:logout"
	PermAdminStatsRead          = "admin_stats:read"
	PermActivityLogsRead        = "activity_logs:read"
	PermInstrumentsImport       = "instruments:import"
	PermInstrumentsRead         = "instruments:read"
	PermInstrumentsCreate       = "instruments:create"
	PermInstrumentsUpdate       = "instruments:update"
	PermInstrumentsDelete       = "instruments:delete"
	PermGlobalInstrumentsRead   = "global_instruments:read"
	PermGlobalInstrumentsImport = "global_instruments:import"
	PermGlobalInstrumentsCreate = "global_instruments:create"
	PermGlobalInstrumentsUpdate = "global_instruments:update"
	PermZerodhaSessionRead      = "zerodha_session:read"
	PermZerodhaSessionUpdate    = "zerodha_session:update"
)

var DefaultAdminPermissions = []string{
	PermUsersCreate,
	PermUsersRead,
	PermUsersUpdate,
	PermUsersDelete,
	PermUsersChangePassword,
	PermUsersLogout,
	PermAdminStatsRead,
	PermActivityLogsRead,
	PermInstrumentsImport,
	PermInstrumentsRead,
	PermInstrumentsCreate,
	PermInstrumentsUpdate,
	PermInstrumentsDelete,
	PermGlobalInstrumentsRead,
	PermGlobalInstrumentsImport,
	PermGlobalInstrumentsCreate,
	PermGlobalInstrumentsUpdate,
	PermZerodhaSessionRead,
	PermZerodhaSessionUpdate,
}

var DefaultAdminPermissionSet = func() map[string]struct{} {
	set := make(map[string]struct{}, len(DefaultAdminPermissions))
	for _, perm := range DefaultAdminPermissions {
		set[perm] = struct{}{}
	}
	return set
}()

func AdminOrSuperAdminAuth(c *fiber.Ctx) error {
	claimsValue := c.Locals("jwt_claims")
	_, ok := claimsValue.(*UserJWTClaims)
	if !ok {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid jwt claims",
			"error":       "invalid jwt claims",
		})
	}

	userValue := c.Locals("user")
	user, ok := userValue.(*models.User)
	if !ok || user == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid authenticated user",
			"error":       "invalid authenticated user",
		})
	}

	if !user.IsAdminRole() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"message":     "admin or super admin access required",
			"error":       "admin or super admin access required",
		})
	}

	return c.Next()
}

func RequireAdminPermission(db *gorm.DB, permission string) fiber.Handler {
	perm := strings.ToLower(strings.TrimSpace(permission))
	return func(c *fiber.Ctx) error {
		userValue := c.Locals("user")
		user, ok := userValue.(*models.User)
		if !ok || user == nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "invalid authenticated user",
				"error":       "invalid authenticated user",
			})
		}

		if user.IsSuperAdminRole() {
			return c.Next()
		}

		has, err := models.UserHasPermission(db, user.ID, perm)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status_code": fiber.StatusInternalServerError,
				"message":     "failed to verify admin permission",
				"error":       "failed to verify admin permission",
			})
		}
		if !has {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"status_code": fiber.StatusForbidden,
				"message":     "permission denied",
				"error":       "permission denied",
				"permission":  perm,
			})
		}

		return c.Next()
	}
}

func SuperAdminOnlyAuth(c *fiber.Ctx) error {
	userValue := c.Locals("user")
	user, ok := userValue.(*models.User)
	if !ok || user == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid authenticated user",
			"error":       "invalid authenticated user",
		})
	}

	if !user.IsSuperAdminRole() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"message":     "super admin access required",
			"error":       "super admin access required",
		})
	}

	return c.Next()
}

// Backward compatibility alias used by existing code paths.
func AdminOnlyAuth(c *fiber.Ctx) error {
	return AdminOrSuperAdminAuth(c)
}
