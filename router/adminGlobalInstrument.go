package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminGlobalInstrumentRoutes(app *fiber.App, globalInstrumentController *controller.AdminGlobalInstrumentController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOnlyAuth)
	adminRoutes.Get("/global-instruments", validator.ValidateListGlobalInstrumentsQuery, globalInstrumentController.List)
	adminRoutes.Post("/global-instruments", globalInstrumentController.Create)
	adminRoutes.Put("/global-instruments/:id", globalInstrumentController.Update)
}
