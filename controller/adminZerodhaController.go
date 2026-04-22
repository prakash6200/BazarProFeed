package controller

import (
	"errors"
	"feedprovider/models"
	"feedprovider/services"
	"feedprovider/validator"
	"log"
	"strings"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type AdminZerodhaController struct {
	db          *gorm.DB
	zerodhaFeed *services.ZerodhaFeedService
}

func NewAdminZerodhaController(db *gorm.DB, zerodhaFeed *services.ZerodhaFeedService) *AdminZerodhaController {
	return &AdminZerodhaController{
		db:          db,
		zerodhaFeed: zerodhaFeed,
	}
}

func (azc *AdminZerodhaController) UpdateSession(c *fiber.Ctx) error {
	if azc.zerodhaFeed == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "zerodha service is not configured",
			"error":       "zerodha service is not configured",
		})
	}

	req := c.Locals("validated_request").(validator.UpdateZerodhaSessionRequest)
	adminUser := c.Locals("user").(*models.User)

	session, err := azc.zerodhaFeed.UpdateAccessTokenFromRequestToken(req.RequestToken, adminUser.ID)
	if err != nil {
		log.Printf("failed to update zerodha session by admin %s: %v", adminUser.ID, err)

		statusCode := fiber.StatusInternalServerError
		errorMessage := strings.TrimSpace(err.Error())
		switch {
		case errors.Is(err, services.ErrZerodhaAuthFailed), errors.Is(err, services.ErrAccessTokenExpired):
			statusCode = fiber.StatusBadRequest
		case strings.Contains(errorMessage, "request_token is required"):
			statusCode = fiber.StatusBadRequest
		case strings.Contains(errorMessage, "ZERODHA_API_KEY"), strings.Contains(errorMessage, "ZERODHA_API_SECRET"):
			statusCode = fiber.StatusInternalServerError
		}

		return c.Status(statusCode).JSON(fiber.Map{
			"status_code": statusCode,
			"message":     errorMessage,
			"error":       errorMessage,
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "zerodha access token updated successfully",
		"session":     buildZerodhaSessionResponse(session),
	})
}

func (azc *AdminZerodhaController) GetSessionStatus(c *fiber.Ctx) error {
	db, cancel := scopedDB(c, azc.db, adminDBTimeout)
	defer cancel()

	session, err := models.GetActiveZerodhaSession(db)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(fiber.StatusOK).JSON(fiber.Map{
				"status_code": fiber.StatusOK,
				"message":     "zerodha session is not configured",
				"configured":  false,
			})
		}
		if isDBBusyErr(err) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(dbBusyJSON())
		}

		log.Printf("failed to fetch zerodha session status: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"status_code": fiber.StatusInternalServerError,
			"message":     "failed to fetch zerodha session status",
			"error":       "failed to fetch zerodha session status",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status_code": fiber.StatusOK,
		"message":     "zerodha session status fetched successfully",
		"configured":  true,
		"session":     buildZerodhaSessionResponse(session),
	})
}

func buildZerodhaSessionResponse(session *models.ZerodhaSession) fiber.Map {
	updatedByAdminID := ""
	if session.UpdatedByAdminID != nil {
		updatedByAdminID = *session.UpdatedByAdminID
	}

	response := fiber.Map{
		"id":                  session.ID,
		"access_token_masked": models.MaskToken(session.AccessToken),
		"generated_at":        session.GeneratedAt,
		"updated_by_admin_id": updatedByAdminID,
		"is_active":           session.IsActive,
		"last_error":          session.LastError,
		"created_at":          session.CreatedAt,
		"updated_at":          session.UpdatedAt,
	}

	if session.UpdatedByAdmin != nil {
		response["updated_by_admin_username"] = session.UpdatedByAdmin.Username
	}

	return response
}
