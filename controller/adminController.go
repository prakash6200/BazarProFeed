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

	lookupDB, lookupCancel := scopedDB(c, ac.db, adminDBTimeout)
	var existingUser models.User
	lookupErr := lookupDB.Where("username = ?", req.Username).First(&existingUser).Error
	lookupCancel()
	if lookupErr == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"status_code": fiber.StatusConflict,
			"message":     "username already exists",
			"error":       "username already exists",
		})
	} else if isDBBusyErr(lookupErr) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
	} else if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
		log.Printf("error checking existing username: %v", lookupErr)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to validate username",
			"error":       "failed to validate username",
		})
	}

	txDB, txCancel := scopedDB(c, ac.db, adminWriteTxTimeout)
	defer txCancel()

	var createdUser *models.User
	err := txDB.Transaction(func(tx *gorm.DB) error {
		user, err := models.CreateUserWithPassword(tx, req.Username, req.Password, targetRole)
		if err != nil {
			return err
		}
		if targetRole == models.RoleAdmin {
			if err := models.GrantPermissions(tx, user.ID, middleware.DefaultAdminPermissions); err != nil {
				return err
			}
		}
		createdUser = user
		return nil
	})
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		log.Printf("error creating user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to create user",
			"error":       "failed to create user",
		})
	}

	middleware.InvalidateUserPermissionCache(createdUser.ID)

	log.Printf("user created: username=%s, id=%s, role=%s, created_by=%s", createdUser.Username, createdUser.ID, createdUser.EffectiveRole(), actor.ID)

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status_code": fiber.StatusCreated,
		"message":     "user created successfully",
		"user": fiber.Map{
			"id":                 createdUser.ID,
			"username":           createdUser.Username,
			"role":               createdUser.EffectiveRole(),
			"api_token":          createdUser.APIToken,
			"token_generated_at": createdUser.TokenGeneratedAt,
			"is_active":          createdUser.IsActive,
			"created_at":         createdUser.CreatedAt,
		},
	})
}

func (ac *AdminController) GetAllUsers(c *fiber.Ctx) error {
	page, limit, err := parsePageLimit(c, 20, 200)
	if err != nil {
		return err
	}

	db, cancel := scopedDB(c, ac.db, adminAnalyticsTimeout)
	users, total, err := models.GetAllUsersPaginated(db, page, limit)
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
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

	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(limit) - 1) / int64(limit))
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "users fetched successfully",
		"users":       userList,
		"pagination": fiber.Map{
			"page":          page,
			"limit":         limit,
			"total_records": total,
			"total_pages":   totalPages,
		},
	})
}

func (ac *AdminController) GetUser(c *fiber.Ctx) error {
	userID := c.Params("id")

	db, cancel := scopedDB(c, ac.db, adminDBTimeout)
	user, err := models.GetUserByID(db, userID)
	cancel()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
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

	lookupDB, lookupCancel := scopedDB(c, ac.db, adminDBTimeout)
	user, err := models.GetUserByID(lookupDB, userID)
	lookupCancel()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch user",
			"error":       "failed to fetch user",
		})
	}

	updateDB, updateCancel := scopedDB(c, ac.db, adminDBTimeout)
	updErr := models.UpdateUserStatus(updateDB, userID, req.IsActive)
	updateCancel()
	if updErr != nil {
		if isDBBusyErr(updErr) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		log.Printf("error updating user status: %v", updErr)
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

	lookupDB, lookupCancel := scopedDB(c, ac.db, adminDBTimeout)
	user, err := models.GetUserByID(lookupDB, userID)
	lookupCancel()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
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

	delDB, delCancel := scopedDB(c, ac.db, adminDBTimeout)
	delErr := models.DeleteUser(delDB, userID)
	delCancel()
	if delErr != nil {
		if isDBBusyErr(delErr) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		log.Printf("error deleting user: %v", delErr)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to delete user",
			"error":       "failed to delete user",
		})
	}

	middleware.InvalidateUserPermissionCache(user.ID)

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

	db, cancel := scopedDB(c, ac.db, adminDBTimeout)
	err := user.RefreshToken(db)
	cancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
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

	txDB, txCancel := scopedDB(c, ac.db, adminWriteTxTimeout)
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
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
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

	lookupDB, lookupCancel := scopedDB(c, ac.db, adminDBTimeout)
	targetUser, err := models.GetUserByID(lookupDB, targetUserID)
	lookupCancel()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
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

	txDB, txCancel := scopedDB(c, ac.db, adminWriteTxTimeout)
	err = txDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.User{}).Where("id = ?", targetUser.ID).Update("password_hash", newPasswordHash).Error; err != nil {
			return err
		}

		if err := targetUser.RefreshToken(tx); err != nil {
			return err
		}

		return nil
	})
	txCancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
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

	lookupDB, lookupCancel := scopedDB(c, ac.db, adminDBTimeout)
	targetUser, err := models.GetUserByID(lookupDB, targetUserID)
	lookupCancel()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
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

	permDB, permCancel := scopedDB(c, ac.db, adminDBTimeout)
	permissions, err := models.ListPermissionStatesByUserID(permDB, targetUserID, middleware.DefaultAdminPermissions)
	permCancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
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

	lookupDB, lookupCancel := scopedDB(c, ac.db, adminDBTimeout)
	targetUser, err := models.GetUserByID(lookupDB, targetUserID)
	lookupCancel()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"status_code": fiber.StatusNotFound,
				"message":     "user not found",
				"error":       "user not found",
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
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

	replaceDB, replaceCancel := scopedDB(c, ac.db, adminWriteTxTimeout)
	replaceErr := models.ReplacePermissionStatesForUser(replaceDB, targetUser.ID, replacementStates)
	replaceCancel()
	if replaceErr != nil {
		if isDBBusyErr(replaceErr) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to update admin permissions",
			"error":       "failed to update admin permissions",
		})
	}

	middleware.InvalidateUserPermissionCache(targetUser.ID)

	fetchDB, fetchCancel := scopedDB(c, ac.db, adminDBTimeout)
	updated, err := models.ListPermissionStatesByUserID(fetchDB, targetUser.ID, middleware.DefaultAdminPermissions)
	fetchCancel()
	if err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
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
	db, cancel := scopedDB(c, ac.db, adminDBTimeout)
	defer cancel()

	var totalUsers int64
	if err := db.Model(&models.User{}).Count(&totalUsers).Error; err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		log.Printf("error counting total users: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch stats",
			"error":       "failed to fetch stats",
		})
	}

	var activeUsers int64
	if err := db.Model(&models.User{}).Where("is_active = ?", true).Count(&activeUsers).Error; err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		log.Printf("error counting active users: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch stats",
			"error":       "failed to fetch stats",
		})
	}

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
	page, limit, err := parsePageLimit(c, 20, 200)
	if err != nil {
		return err
	}

	// Length-bound every free-text search parameter. Without these,
	// a client can pass a 10 MB string and make Postgres do an ILIKE
	// across every row of the audit log — which then eats the pool.
	userIDFilter := strings.TrimSpace(c.Query("user_id"))
	if len(userIDFilter) > 64 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "user_id is too long",
			"error":       "user_id is too long",
		})
	}

	pathFilter := strings.TrimSpace(c.Query("path"))
	if len(pathFilter) > 200 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "path filter is too long",
			"error":       "path filter is too long",
		})
	}

	db, cancel := scopedDB(c, ac.db, adminAnalyticsTimeout)
	defer cancel()

	query := db.Model(&models.AdminAPIAuditLog{})

	if userIDFilter != "" {
		query = query.Where("user_id = ?", userIDFilter)
	}
	if role := strings.ToUpper(strings.TrimSpace(c.Query("role"))); role != "" {
		query = query.Where("role = ?", role)
	}
	if method := strings.ToUpper(strings.TrimSpace(c.Query("method"))); method != "" {
		query = query.Where("method = ?", method)
	}
	if pathFilter != "" {
		query = query.Where("path ILIKE ?", "%"+pathFilter+"%")
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
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch activity logs",
			"error":       "failed to fetch activity logs",
		})
	}

	offset := (page - 1) * limit
	var logs []models.AdminAPIAuditLog
	if err := query.Order("created_at DESC").Offset(offset).Limit(limit).Find(&logs).Error; err != nil {
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}
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
