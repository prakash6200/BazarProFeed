package controller

import (
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

// CreateUser creates a new user
// POST /admin/users
func (ac *AdminController) CreateUser(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.CreateUserRequest)

	// Check if username already exists
	var existingUser models.User
	if err := ac.db.Where("username = ?", req.Username).First(&existingUser).Error; err == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error": "username already exists",
		})
	}

	// Create user
	user, err := models.CreateUser(ac.db, req.Username, false)
	if err != nil {
		log.Printf("error creating user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to create user",
		})
	}

	log.Printf("user created: username=%s, id=%s", user.Username, user.ID)

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "user created successfully",
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

// GetAllUsers retrieves all users
// GET /admin/users
func (ac *AdminController) GetAllUsers(c *fiber.Ctx) error {
	users, err := models.GetAllUsers(ac.db)
	if err != nil {
		log.Printf("error fetching users: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to fetch users",
		})
	}

	// Format response
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

	return c.JSON(fiber.Map{
		"users": userList,
		"count": len(users),
	})
}

// GetUser retrieves a single user by ID
// GET /admin/users/:id
func (ac *AdminController) GetUser(c *fiber.Ctx) error {
	userID := c.Params("id")

	user, err := models.GetUserByID(ac.db, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "user not found",
			})
		}
		log.Printf("error fetching user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to fetch user",
		})
	}

	return c.JSON(fiber.Map{
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

// UpdateUserStatus updates user's active status
// PUT /admin/users/:id/status
func (ac *AdminController) UpdateUserStatus(c *fiber.Ctx) error {
	userID := c.Params("id")
	req := c.Locals("validated_request").(validator.UpdateStatusRequest)

	// Check if user exists
	user, err := models.GetUserByID(ac.db, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to fetch user",
		})
	}

	// Update status
	if err := models.UpdateUserStatus(ac.db, userID, req.IsActive); err != nil {
		log.Printf("error updating user status: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to update user status",
		})
	}

	// If user is being disabled, close all their websocket connections
	if !req.IsActive {
		ac.socketHub.CloseUserConnections(user.APIToken)
		log.Printf("user disabled and connections closed: username=%s, id=%s", user.Username, userID)
	}

	log.Printf("user status updated: username=%s, id=%s, is_active=%v", user.Username, userID, req.IsActive)

	return c.JSON(fiber.Map{
		"message":   "user status updated successfully",
		"is_active": req.IsActive,
	})
}

// DeleteUser deletes a user
// DELETE /admin/users/:id
func (ac *AdminController) DeleteUser(c *fiber.Ctx) error {
	userID := c.Params("id")

	// Check if user exists and is not admin
	user, err := models.GetUserByID(ac.db, userID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "user not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to fetch user",
		})
	}

	// Prevent deletion of admin users
	if user.IsAdmin {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "cannot delete admin user",
		})
	}

	// Close user's connections before deleting
	ac.socketHub.CloseUserConnections(user.APIToken)

	// Delete user
	if err := models.DeleteUser(ac.db, userID); err != nil {
		log.Printf("error deleting user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to delete user",
		})
	}

	log.Printf("user deleted: username=%s, id=%s", user.Username, userID)

	return c.JSON(fiber.Map{
		"message": "user deleted successfully",
	})
}

// GetStats retrieves system statistics
// GET /admin/stats
func (ac *AdminController) GetStats(c *fiber.Ctx) error {
	// Count total users
	var totalUsers int64
	ac.db.Model(&models.User{}).Count(&totalUsers)

	// Count active users
	var activeUsers int64
	ac.db.Model(&models.User{}).Where("is_active = ?", true).Count(&activeUsers)

	// Get connected websocket clients count
	connectedClients := ac.socketHub.GetClientCount()

	return c.JSON(fiber.Map{
		"stats": fiber.Map{
			"total_users":       totalUsers,
			"active_users":      activeUsers,
			"connected_clients": connectedClients,
		},
	})
}
