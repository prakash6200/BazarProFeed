package middleware

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

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

// permCacheTTL bounds how long we trust a cached permission check.
// 30s gives us enough absorption against permission-check storms
// (every admin request hits this) without materially delaying a
// revoke/grant taking effect. Keyed by "userID|perm" so a single
// evicted entry only affects one user/permission pair.
const (
	permCacheTTL     = 30 * time.Second
	permDBTimeout    = 2 * time.Second
	permNegativeTTL  = 10 * time.Second
	permCacheMaxSize = 4096
)

type permCacheEntry struct {
	allowed   bool
	expiresAt time.Time
}

var (
	permCacheMu sync.RWMutex
	permCache   = make(map[string]permCacheEntry, permCacheMaxSize)
)

func permCacheGet(key string) (bool, bool) {
	permCacheMu.RLock()
	entry, ok := permCache[key]
	permCacheMu.RUnlock()
	if !ok {
		return false, false
	}
	if time.Now().After(entry.expiresAt) {
		return false, false
	}
	return entry.allowed, true
}

func permCacheSet(key string, allowed bool) {
	ttl := permCacheTTL
	if !allowed {
		ttl = permNegativeTTL
	}
	permCacheMu.Lock()
	if len(permCache) >= permCacheMaxSize {
		// Cheap eviction: drop 25% of the map on overflow. Bounded
		// to permCacheMaxSize/4 work per eviction; lookup stays O(1).
		n := 0
		for k := range permCache {
			delete(permCache, k)
			n++
			if n >= permCacheMaxSize/4 {
				break
			}
		}
	}
	permCache[key] = permCacheEntry{
		allowed:   allowed,
		expiresAt: time.Now().Add(ttl),
	}
	permCacheMu.Unlock()
}

// InvalidatePermissionCache clears all cached permission decisions.
// Call after admin permission mutations so changes take effect within
// the next request cycle.
func InvalidatePermissionCache() {
	permCacheMu.Lock()
	permCache = make(map[string]permCacheEntry, permCacheMaxSize)
	permCacheMu.Unlock()
}

// InvalidateUserPermissionCache drops every cached entry for one
// user. Prefer this over the global invalidation when possible.
func InvalidateUserPermissionCache(userID string) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	prefix := userID + "|"
	permCacheMu.Lock()
	for k := range permCache {
		if strings.HasPrefix(k, prefix) {
			delete(permCache, k)
		}
	}
	permCacheMu.Unlock()
}

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

		cacheKey := user.ID + "|" + perm
		if allowed, hit := permCacheGet(cacheKey); hit {
			if !allowed {
				return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
					"status_code": fiber.StatusForbidden,
					"message":     "permission denied",
					"error":       "permission denied",
					"permission":  perm,
				})
			}
			return c.Next()
		}

		lookupCtx, cancel := context.WithTimeout(c.UserContext(), permDBTimeout)
		has, err := models.UserHasPermission(db.WithContext(lookupCtx), user.ID, perm)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
					"status_code": fiber.StatusServiceUnavailable,
					"message":     "permission service busy",
					"error":       "permission service busy",
				})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status_code": fiber.StatusInternalServerError,
				"message":     "failed to verify admin permission",
				"error":       "failed to verify admin permission",
			})
		}
		permCacheSet(cacheKey, has)
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
