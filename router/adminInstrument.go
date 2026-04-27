package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminInstrumentRoutes(app *fiber.App, instrumentController *controller.AdminInstrumentController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOrSuperAdminAuth, middleware.AdminAPIAccessAudit(db))
	adminRoutes.Post("/instruments/import", middleware.RequireAdminPermission(db, middleware.PermInstrumentsImport), validator.ValidateImportInstruments, instrumentController.ImportInstruments)
	adminRoutes.Get("/instruments", middleware.RequireAdminPermission(db, middleware.PermInstrumentsRead), validator.ValidateListInstrumentsQuery, instrumentController.GetInstruments)
	adminRoutes.Get("/instruments/:id", middleware.RequireAdminPermission(db, middleware.PermInstrumentsRead), validator.ValidateInstrumentID, instrumentController.GetInstrument)
	adminRoutes.Post("/instruments", middleware.RequireAdminPermission(db, middleware.PermInstrumentsCreate), validator.ValidateCreateInstrument, instrumentController.CreateInstrument)
	adminRoutes.Put("/instruments/bulk", middleware.RequireAdminPermission(db, middleware.PermInstrumentsUpdate), validator.ValidateBulkUpdateInstruments, instrumentController.BulkUpdateInstruments)
	adminRoutes.Put("/instruments/:id", middleware.RequireAdminPermission(db, middleware.PermInstrumentsUpdate), validator.ValidateInstrumentID, validator.ValidateUpdateInstrument, instrumentController.UpdateInstrument)
	adminRoutes.Delete("/instruments/:id", middleware.RequireAdminPermission(db, middleware.PermInstrumentsDelete), validator.ValidateInstrumentID, instrumentController.DeleteInstrument)
}
