package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminGlobalInstrumentRoutes(app *fiber.App, globalInstrumentController *controller.AdminGlobalInstrumentController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOrSuperAdminAuth, middleware.AdminAPIAccessAudit(db))
	adminRoutes.Get("/global-instruments", middleware.RequireAdminPermission(db, middleware.PermGlobalInstrumentsRead), validator.ValidateListGlobalInstrumentsQuery, globalInstrumentController.List)
	adminRoutes.Post("/global-instruments/import", middleware.RequireAdminPermission(db, middleware.PermGlobalInstrumentsImport), validator.ValidateImportGlobalInstruments, globalInstrumentController.Import)
	adminRoutes.Post("/global-instruments", middleware.RequireAdminPermission(db, middleware.PermGlobalInstrumentsCreate), globalInstrumentController.Create)
	adminRoutes.Put("/global-instruments/:id", middleware.RequireAdminPermission(db, middleware.PermGlobalInstrumentsUpdate), globalInstrumentController.Update)
}
