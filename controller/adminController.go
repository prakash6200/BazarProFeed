package controller

import (
	"errors"
	"feedprovider/config"
	"feedprovider/models"
	"feedprovider/validator"
	"log"

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

	user, err := models.CreateUser(ac.db, req.Username, targetRole)
	if err != nil {
		log.Printf("error creating user: %v", err)
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
