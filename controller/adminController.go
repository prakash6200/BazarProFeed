package controller

import (
	"errors"
	"feedprovider/config"
	"feedprovider/middleware"
	"feedprovider/models"
	"feedprovider/validator"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type AdminController struct {
	db        *gorm.DB
	socketHub *config.SocketHub
}

type updateAdminPermissionsRequest struct {
	Permissions []models.PermissionState `json:"permissions"`
}

func NewAdminController(db *gorm.DB, socketHub *config.SocketHub) *AdminController {
	return &AdminController{
		db:        db,
		socketHub: socketHub,
	}
}

func (ac *AdminController) CreateUser(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.CreateUserRequest)
	actor, ok := c.Locals("user").(*models.User)
	if !ok || actor == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid authenticated user",
			"error":       "invalid authenticated user",
		})
	}

	targetRole := req.Role
	if targetRole == "" {
		targetRole = models.RoleUser
	}

	if actor.IsSuperAdminRole() {
		if targetRole == models.RoleSuperAdmin {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"status_code": fiber.StatusForbidden,
				"message":     "super admin creation is not allowed via this endpoint",
				"error":       "super admin creation is not allowed via this endpoint",
			})
		}
	} else {
		if targetRole != models.RoleUser {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"status_code": fiber.StatusForbidden,
				"message":     "admin can only create USER role",
				"error":       "admin can only create USER role",
			})
		}
	}

	var existingUser models.User
	if err := ac.db.Where("username = ?", req.Username).First(&existingUser).Error; err == nil {
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

	tx := ac.db.Begin()
	if tx.Error != nil {
		log.Printf("error starting transaction for user creation: %v", tx.Error)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to create user",
			"error":       "failed to create user",
		})
	}

	user, err := models.CreateUserWithPassword(tx, req.Username, req.Password, targetRole)
	if err != nil {
		_ = tx.Rollback().Error
		log.Printf("error creating user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to create user",
			"error":       "failed to create user",
		})
	}

	if targetRole == models.RoleAdmin {
		if err := models.GrantPermissions(tx, user.ID, middleware.DefaultAdminPermissions); err != nil {
			_ = tx.Rollback().Error
			log.Printf("error assigning default admin permissions: user_id=%s err=%v", user.ID, err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status_code": fiber.StatusInternalServerError,
				"message":     "failed to assign default admin permissions",
				"error":       "failed to assign default admin permissions",
			})
		}
	}

	if err := tx.Commit().Error; err != nil {
		_ = tx.Rollback().Error
		log.Printf("error committing user creation transaction: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to create user",
			"error":       "failed to create user",
		})
	}

	log.Printf("user created: username=%s, id=%s, role=%s, created_by=%s", user.Username, user.ID, user.EffectiveRole(), actor.ID)

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code": fiber.StatusCreated,
		"message":     "user created successfully",
		"user": fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"role":               user.EffectiveRole(),
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
			"message":     "failed to fetch users",
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
			"role":               user.EffectiveRole(),
			"created_at":         user.CreatedAt,
			"updated_at":         user.UpdatedAt,
		}
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "users fetched successfully",
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
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		log.Printf("error fetching user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch user",
			"error":       "failed to fetch user",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "user fetched successfully",
		"user": fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"api_token":          user.APIToken,
			"token_generated_at": user.TokenGeneratedAt,
			"is_active":          user.IsActive,
			"role":               user.EffectiveRole(),
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
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch user",
			"error":       "failed to fetch user",
		})
	}

	if err := models.UpdateUserStatus(ac.db, userID, req.IsActive); err != nil {
		log.Printf("error updating user status: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update user status",
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
	actor, ok := c.Locals("user").(*models.User)
	if !ok || actor == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid authenticated user",
			"error":       "invalid authenticated user",
		})
	}

	user, err := models.GetUserByID(ac.db, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch user",
			"error":       "failed to fetch user",
		})
	}

	if user.ID == actor.ID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"message":     "cannot delete your own account",
			"error":       "cannot delete your own account",
		})
	}

	if !actor.IsSuperAdminRole() && user.IsAdminRole() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"message":     "admin cannot delete ADMIN or SUPER_ADMIN users",
			"error":       "admin cannot delete ADMIN or SUPER_ADMIN users",
		})
	}

	ac.socketHub.CloseUserConnections(user.ID)

	if err := models.DeleteUser(ac.db, userID); err != nil {
		log.Printf("error deleting user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to delete user",
			"error":       "failed to delete user",
		})
	}

	log.Printf("user deleted: username=%s, id=%s", user.Username, userID)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "user deleted successfully",
	})
}

func (ac *AdminController) Logout(c *fiber.Ctx) error {
	user, ok := c.Locals("user").(*models.User)
	if !ok || user == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid authenticated user",
			"error":       "invalid authenticated user",
		})
	}

	ac.socketHub.CloseUserConnections(user.ID)

	if err := user.RefreshToken(ac.db); err != nil {
		log.Printf("error logging out admin %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to logout",
			"error":       "failed to logout",
		})
	}

	log.Printf("admin logged out: username=%s, id=%s", user.Username, user.ID)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "logout successful",
	})
}

func (ac *AdminController) ChangePassword(c *fiber.Ctx) error {
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
		log.Printf("error hashing new password for admin %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update password",
			"error":       "failed to update password",
		})
	}

	err = ac.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.User{}).Where("id = ?", user.ID).Update("password_hash", newPasswordHash).Error; err != nil {
			return err
		}

		if err := user.RefreshToken(tx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Printf("error changing password for admin %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to change password",
			"error":       "failed to change password",
		})
	}

	ac.socketHub.CloseUserConnections(user.ID)
	log.Printf("admin password changed: username=%s, id=%s", user.Username, user.ID)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "password changed successfully, please login again",
	})
}

func (ac *AdminController) ChangeUserPassword(c *fiber.Ctx) error {
	targetUserID := strings.TrimSpace(c.Params("id"))
	if targetUserID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "user id is required",
			"error":       "user id is required",
		})
	}

	actor, ok := c.Locals("user").(*models.User)
	if !ok || actor == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"message":     "invalid authenticated user",
			"error":       "invalid authenticated user",
		})
	}

	req := c.Locals("validated_request").(validator.SuperAdminChangeUserPasswordRequest)

	targetUser, err := models.GetUserByID(ac.db, targetUserID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch user",
			"error":       "failed to fetch user",
		})
	}

	if targetUser.IsSuperAdminRole() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"message":     "super admin password cannot be changed from this endpoint",
			"error":       "super admin password cannot be changed from this endpoint",
		})
	}

	if targetUser.PasswordHash != "" && models.CheckPasswordHash(req.NewPassword, targetUser.PasswordHash) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "new_password must be different from current password",
			"error":       "new_password must be different from current password",
		})
	}

	newPasswordHash, err := models.HashPassword(req.NewPassword)
	if err != nil {
		log.Printf("error hashing new password for user %s: %v", targetUser.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update password",
			"error":       "failed to update password",
		})
	}

	err = ac.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.User{}).Where("id = ?", targetUser.ID).Update("password_hash", newPasswordHash).Error; err != nil {
			return err
		}

		if err := targetUser.RefreshToken(tx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Printf("error changing password by super admin: actor=%s target=%s err=%v", actor.Username, targetUser.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to change password",
			"error":       "failed to change password",
		})
	}

	ac.socketHub.CloseUserConnections(targetUser.ID)
	log.Printf("user password changed by super admin: actor=%s target=%s target_role=%s", actor.Username, targetUser.Username, targetUser.EffectiveRole())

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "password changed successfully",
		"user": fiber.Map{
			"id":       targetUser.ID,
			"username": targetUser.Username,
			"role":     targetUser.EffectiveRole(),
		},
	})
}

func (ac *AdminController) GetAdminPermissions(c *fiber.Ctx) error {
	targetUserID := strings.TrimSpace(c.Params("id"))
	if targetUserID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "user id is required",
			"error":       "user id is required",
		})
	}

	targetUser, err := models.GetUserByID(ac.db, targetUserID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch user",
			"error":       "failed to fetch user",
		})
	}

	if targetUser.EffectiveRole() != models.RoleAdmin {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "permissions can only be managed for ADMIN role",
			"error":       "permissions can only be managed for ADMIN role",
		})
	}

	permissions, err := models.ListPermissionStatesByUserID(ac.db, targetUserID, middleware.DefaultAdminPermissions)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch admin permissions",
			"error":       "failed to fetch admin permissions",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "admin permissions fetched successfully",
		"user": fiber.Map{
			"id":       targetUser.ID,
			"username": targetUser.Username,
			"role":     targetUser.EffectiveRole(),
		},
		"permissions": permissions,
	})
}

func (ac *AdminController) UpdateAdminPermissions(c *fiber.Ctx) error {
	targetUserID := strings.TrimSpace(c.Params("id"))
	if targetUserID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "user id is required",
			"error":       "user id is required",
		})
	}

	targetUser, err := models.GetUserByID(ac.db, targetUserID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch user",
			"error":       "failed to fetch user",
		})
	}

	if targetUser.EffectiveRole() != models.RoleAdmin {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "permissions can only be managed for ADMIN role",
			"error":       "permissions can only be managed for ADMIN role",
		})
	}

	var req updateAdminPermissionsRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "invalid request body",
			"error":       "invalid request body",
		})
	}

	defaultState := make(map[string]bool, len(middleware.DefaultAdminPermissions))
	for _, perm := range middleware.DefaultAdminPermissions {
		defaultState[strings.ToLower(strings.TrimSpace(perm))] = false
	}

	seen := make(map[string]struct{}, len(req.Permissions))
	for _, permState := range req.Permissions {
		norm := strings.ToLower(strings.TrimSpace(permState.Permission))
		if norm == "" {
			continue
		}
		if _, ok := middleware.DefaultAdminPermissionSet[norm]; !ok {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "invalid permission: " + norm,
				"error":       "invalid permission: " + norm,
			})
		}
		if _, exists := seen[norm]; exists {
			continue
		}
		seen[norm] = struct{}{}
		defaultState[norm] = permState.Allowed
	}

	replacementStates := make([]models.PermissionState, 0, len(middleware.DefaultAdminPermissions))
	for _, perm := range middleware.DefaultAdminPermissions {
		norm := strings.ToLower(strings.TrimSpace(perm))
		replacementStates = append(replacementStates, models.PermissionState{
			Permission: norm,
			Allowed:    defaultState[norm],
		})
	}

	if err := models.ReplacePermissionStatesForUser(ac.db, targetUser.ID, replacementStates); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update admin permissions",
			"error":       "failed to update admin permissions",
		})
	}

	updated, err := models.ListPermissionStatesByUserID(ac.db, targetUser.ID, middleware.DefaultAdminPermissions)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "permissions updated but fetch failed",
			"error":       "permissions updated but fetch failed",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "admin permissions updated successfully",
		"user": fiber.Map{
			"id":       targetUser.ID,
			"username": targetUser.Username,
			"role":     targetUser.EffectiveRole(),
		},
		"permissions": updated,
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
		"message":     "stats fetched successfully",
		"stats": fiber.Map{
			"total_users":       totalUsers,
			"active_users":      activeUsers,
			"connected_clients": connectedClients,
		},
	})
}

func (ac *AdminController) GetActivityLogs(c *fiber.Ctx) error {
	page := 1
	if rawPage := strings.TrimSpace(c.Query("page")); rawPage != "" {
		parsed, err := strconv.Atoi(rawPage)
		if err != nil || parsed < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "page must be a positive integer",
				"error":       "page must be a positive integer",
			})
		}
		page = parsed
	}

	limit := 20
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be a positive integer",
				"error":       "limit must be a positive integer",
			})
		}
		if parsed > 200 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be less than or equal to 200",
				"error":       "limit must be less than or equal to 200",
			})
		}
		limit = parsed
	}

	query := ac.db.Model(&models.AdminAPIAuditLog{})

	if userID := strings.TrimSpace(c.Query("user_id")); userID != "" {
		query = query.Where("user_id = ?", userID)
	}
	if role := strings.ToUpper(strings.TrimSpace(c.Query("role"))); role != "" {
		query = query.Where("role = ?", role)
	}
	if method := strings.ToUpper(strings.TrimSpace(c.Query("method"))); method != "" {
		query = query.Where("method = ?", method)
	}
	if path := strings.TrimSpace(c.Query("path")); path != "" {
		query = query.Where("path ILIKE ?", "%"+path+"%")
	}
	if statusCodeRaw := strings.TrimSpace(c.Query("status_code")); statusCodeRaw != "" {
		statusCode, err := strconv.Atoi(statusCodeRaw)
		if err != nil || statusCode < 100 || statusCode > 599 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "status_code must be a valid HTTP status code",
				"error":       "status_code must be a valid HTTP status code",
			})
		}
		query = query.Where("status_code = ?", statusCode)
	}

	if from := strings.TrimSpace(c.Query("from")); from != "" {
		parsedFrom, err := time.Parse(time.RFC3339, from)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "from must be in RFC3339 format",
				"error":       "from must be in RFC3339 format",
			})
		}
		query = query.Where("created_at >= ?", parsedFrom.UTC())
	}

	if to := strings.TrimSpace(c.Query("to")); to != "" {
		parsedTo, err := time.Parse(time.RFC3339, to)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "to must be in RFC3339 format",
				"error":       "to must be in RFC3339 format",
			})
		}
		query = query.Where("created_at <= ?", parsedTo.UTC())
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch activity logs",
			"error":       "failed to fetch activity logs",
		})
	}

	offset := (page - 1) * limit
	var logs []models.AdminAPIAuditLog
	if err := query.Order("created_at DESC").Offset(offset).Limit(limit).Find(&logs).Error; err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch activity logs",
			"error":       "failed to fetch activity logs",
		})
	}

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(limit) - 1) / int64(limit))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "activity logs fetched successfully",
		"logs":        logs,
		"pagination": fiber.Map{
			"page":          page,
			"limit":         limit,
			"total_records": total,
			"total_pages":   totalPages,
		},
	})
}
