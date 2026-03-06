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
