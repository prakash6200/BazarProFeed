package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminZerodhaRoutes(app *fiber.App, zerodhaController *controller.AdminZerodhaController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOnlyAuth)
	adminRoutes.Get("/zerodha/session/status", zerodhaController.GetSessionStatus)
	adminRoutes.Post("/zerodha/session", validator.ValidateUpdateZerodhaSession, zerodhaController.UpdateSession)
}
