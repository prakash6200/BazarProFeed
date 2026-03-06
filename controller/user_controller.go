package controller

import (
	"errors"
	"feedprovider/config"
	"feedprovider/middleware"
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

func (uc *UserController) Signup(c *fiber.Ctx) error {
	req := c.Locals("validated_request").(validator.SignupRequest)

	if _, err := models.GetUserByUsername(uc.db, req.Username); err == nil {
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

	user, err := models.CreateUserWithPassword(uc.db, req.Username, req.Password, false)
	if err != nil {
		log.Printf("error creating user signup: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to signup",
		})
	}

	jwtToken, err := middleware.GenerateJWT(user)
	if err != nil {
		log.Printf("error generating jwt for signup user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
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

	user, err := models.GetUserByUsername(uc.db, req.Username)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"error":       "invalid credentials",
			})
		}
		log.Printf("error fetching user by username: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "login failed",
		})
	}

	if user.PasswordHash == "" || !models.CheckPasswordHash(req.Password, user.PasswordHash) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"status_code": fiber.StatusUnauthorized,
			"error":       "invalid credentials",
		})
	}

	if !user.IsActive {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"error":       "user account is disabled",
		})
	}

	if err := user.RefreshToken(uc.db); err != nil {
		log.Printf("error refreshing token on login for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to issue login token",
		})
	}

	jwtToken, err := middleware.GenerateJWT(user)
	if err != nil {
		log.Printf("error generating jwt for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
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
	})
}

func (uc *UserController) GetProfile(c *fiber.Ctx) error {
	user := c.Locals("user").(*models.User)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
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
			"error":       "invalid token",
		})
	}

	if !user.IsActive {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"status_code": fiber.StatusForbidden,
			"error":       "user account is disabled",
		})
	}

	uc.socketHub.CloseUserConnections(user.ID)

	if err := user.RefreshToken(uc.db); err != nil {
		log.Printf("error refreshing token for user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"error":       "failed to generate new token",
		})
	}

	log.Printf("token refreshed for user: username=%s, id=%s", user.Username, user.ID)

	jwtToken, err := middleware.GenerateJWT(user)
	if err != nil {
		log.Printf("error generating jwt for refreshed token user %s: %v", user.Username, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
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
