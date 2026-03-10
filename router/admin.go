package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminRoutes(app *fiber.App, adminController *controller.AdminController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOnlyAuth)
	adminRoutes.Post("/users", validator.ValidateCreateUser, adminController.CreateUser)
	adminRoutes.Get("/users", adminController.GetAllUsers)
	adminRoutes.Get("/users/:id", adminController.GetUser)
	adminRoutes.Put("/users/:id/status", validator.ValidateUpdateStatus, adminController.UpdateUserStatus)
	adminRoutes.Delete("/users/:id", adminController.DeleteUser)
	adminRoutes.Post("/instruments/import", validator.ValidateImportInstruments, adminController.ImportInstruments)
	adminRoutes.Post("/instruments", validator.ValidateCreateInstrument, adminController.CreateInstrument)
	adminRoutes.Put("/instruments/:id", validator.ValidateUpdateInstrument, adminController.UpdateInstrument)
	adminRoutes.Delete("/instruments/:id", adminController.DeleteInstrument)
	adminRoutes.Get("/stats", adminController.GetStats)
}
