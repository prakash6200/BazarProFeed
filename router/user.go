package router

import (
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/validator"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func RegisterUserRoutes(app *fiber.App, userController *controller.UserController, db *gorm.DB) {
	authRoutes := app.Group("/auth")
	authRoutes.Post("/signup", validator.ValidateSignup, userController.Signup)
	authRoutes.Post("/login", validator.ValidateLogin, userController.Login)

	userRoutes := app.Group("/api", middleware.UserAuth(db))
	userRoutes.Get("/profile", userController.GetProfile)
	userRoutes.Post("/refresh-token", validator.ValidateRefreshToken, userController.RefreshToken)
}
