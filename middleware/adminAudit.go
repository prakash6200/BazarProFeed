package middleware

import (
	"time"

	"feedprovider/models"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func AdminAPIAccessAudit(db *gorm.DB) fiber.Handler {
	return func(c *fiber.Ctx) error {
		startedAt := time.Now()
		err := c.Next()
		latency := time.Since(startedAt)

		userValue := c.Locals("user")
		user, ok := userValue.(*models.User)
		if !ok || user == nil || !user.IsAdminRole() {
			return err
		}

		statusCode := c.Response().StatusCode()
		errorText := ""
		if err != nil {
			errorText = err.Error()
		}

		_ = models.CreateAdminAPIAuditLog(db, &models.AdminAPIAuditLog{
			UserID:     user.ID,
			Username:   user.Username,
			Role:       user.EffectiveRole(),
			Method:     c.Method(),
			Path:       c.Path(),
			Query:      string(c.Request().URI().QueryString()),
			StatusCode: statusCode,
			IPAddress:  c.IP(),
			UserAgent:  c.Get("User-Agent"),
			LatencyMS:  latency.Milliseconds(),
			ErrorText:  errorText,
		})

		return err
	}
}
