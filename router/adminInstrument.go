package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminInstrumentRoutes(app *fiber.App, instrumentController *controller.AdminInstrumentController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOnlyAuth)
	adminRoutes.Post("/instruments/import", validator.ValidateImportInstruments, instrumentController.ImportInstruments)
	adminRoutes.Post("/instruments", validator.ValidateCreateInstrument, instrumentController.CreateInstrument)
	adminRoutes.Put("/instruments/:id", validator.ValidateUpdateInstrument, instrumentController.UpdateInstrument)
	adminRoutes.Delete("/instruments/:id", instrumentController.DeleteInstrument)
}
