package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminZerodhaRoutes(app *fiber.App, zerodhaController *controller.AdminZerodhaController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOrSuperAdminAuth, middleware.AdminAPIAccessAudit(db))
	adminRoutes.Get("/zerodha/session/status", middleware.RequireAdminPermission(db, middleware.PermZerodhaSessionRead), zerodhaController.GetSessionStatus)
	adminRoutes.Post("/zerodha/session", middleware.RequireAdminPermission(db, middleware.PermZerodhaSessionUpdate), validator.ValidateUpdateZerodhaSession, zerodhaController.UpdateSession)
}
