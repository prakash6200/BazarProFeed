package controller

import (
	"feedprovider/config"
	"feedprovider/models"
	"feedprovider/validator"
	"log"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type UserController struct {
	db        *gorm.DB
	socketHub *config.SocketHub
}

func NewUserController(db *gorm.DB, socketHub *config.SocketHub) *UserController {
	return &UserController{
		db:        db,
		socketHub: socketHub,
	}
}

// GetProfile returns the authenticated user's profile
// GET /api/profile
func (uc *UserController) GetProfile(c *fiber.Ctx) error {
	user := c.Locals("user").(*models.User)

	return c.JSON(fiber.Map{
		"user": fiber.Map{
			"id":                 user.ID,
			"username":           user.Username,
			"token_generated_at": user.TokenGeneratedAt,
			"is_active":          user.IsActive,
			"created_at":         user.CreatedAt,
		},
	})
}

// RefreshToken generates a new token for the user
// POST /api/refresh-token
func (uc *UserController) RefreshToken(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.RefreshTokenRequest)

	// Get user by old token
	user, err := models.GetUserByToken(uc.db, req.OldToken)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid token",
			})
		}
		log.Printf("error fetching user by token: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "token refresh failed",
		})
	}

	// Check if user is active
	if !user.IsActive {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "user account is disabled",
		})
	}

	// Close all websocket connections with old token
	uc.socketHub.CloseUserConnections(req.OldToken)

	// Generate new token
	if err := user.RefreshToken(uc.db); err != nil {
		log.Printf("error refreshing token for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to generate new token",
		})
	}

	log.Printf("token refreshed for user: username=%s, id=%s", user.Username, user.ID)

	return c.JSON(fiber.Map{
		"message": "token refreshed successfully",
		"token": fiber.Map{
			"api_token":          user.APIToken,
			"token_generated_at": user.TokenGeneratedAt,
		},
	})
}
