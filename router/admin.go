package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterAdminRoutes(app *fiber.App, adminController *controller.AdminController, db *gorm.DB) {
	adminRoutes := app.Group("/admin", middleware.UserAuth(db), middleware.AdminOrSuperAdminAuth, middleware.AdminAPIAccessAudit(db))
	adminRoutes.Post("/users", middleware.RequireAdminPermission(db, middleware.PermUsersCreate), validator.ValidateCreateUser, adminController.CreateUser)
	adminRoutes.Get("/users", middleware.RequireAdminPermission(db, middleware.PermUsersRead), adminController.GetAllUsers)
	adminRoutes.Get("/users/:id", middleware.RequireAdminPermission(db, middleware.PermUsersRead), adminController.GetUser)
	adminRoutes.Put("/users/:id/status", middleware.RequireAdminPermission(db, middleware.PermUsersUpdate), validator.ValidateUpdateStatus, adminController.UpdateUserStatus)
	adminRoutes.Delete("/users/:id", middleware.RequireAdminPermission(db, middleware.PermUsersDelete), adminController.DeleteUser)
	adminRoutes.Post("/logout", middleware.RequireAdminPermission(db, middleware.PermUsersLogout), adminController.Logout)
	adminRoutes.Post("/change-password", middleware.RequireAdminPermission(db, middleware.PermUsersChangePassword), validator.ValidateAdminChangePassword, adminController.ChangePassword)
	adminRoutes.Get("/stats", middleware.RequireAdminPermission(db, middleware.PermAdminStatsRead), adminController.GetStats)
	adminRoutes.Get("/admins/:id/permissions", middleware.SuperAdminOnlyAuth, adminController.GetAdminPermissions)
	adminRoutes.Put("/admins/:id/permissions", middleware.SuperAdminOnlyAuth, adminController.UpdateAdminPermissions)
}
