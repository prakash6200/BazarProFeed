package middleware

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"feedprovider/models"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"
)

// authDBTimeout bounds the GetUserByID lookup that runs on every
// /api/* request. Without this, a saturated Postgres pool causes
// sql.DB.conn() to block indefinitely — every authenticated request
// pins a fasthttp worker forever and the server becomes unresponsive
// while the port stays open. Failing fast (503) is the only way to
// let the pool recover under pressure.
const authDBTimeout = 2 * time.Second

type UserJWTClaims struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

func jwtSecret() (string, error) {
	secret := strings.TrimSpace(os.Getenv("JWT_SECRET"))
	if secret == "" {
		return "", errors.New("JWT_SECRET is not configured")
	}
	return secret, nil
}

func GenerateJWT(user *models.User) (string, error) {
	secret, err := jwtSecret()
	if err != nil {
		return "", err
	}

	claims := UserJWTClaims{
		UserID:   user.ID,
		Username: user.Username,
		Role:     user.EffectiveRole(),
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

func UserAuth(db *gorm.DB) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "authorization token required",
				"error":       "authorization token required",
			})
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "invalid authorization format, use: Bearer <token>",
				"error":       "invalid authorization format, use: Bearer <token>",
			})
		}

		token := parts[1]
		secret, err := jwtSecret()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status_code": fiber.StatusInternalServerError,
				"message":     "jwt authentication is not configured",
				"error":       "jwt authentication is not configured",
			})
		}

		claims := &UserJWTClaims{}
		parsedToken, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, errors.New("unexpected signing method")
			}
			return []byte(secret), nil
		})

		if err != nil || !parsedToken.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "invalid jwt token",
				"error":       "invalid jwt token",
			})
		}

		if claims.UserID == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "invalid jwt claims",
				"error":       "invalid jwt claims",
			})
		}

		lookupCtx, cancel := context.WithTimeout(c.UserContext(), authDBTimeout)
		user, err := models.GetUserByID(db.WithContext(lookupCtx), claims.UserID)
		cancel()
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"status_code": fiber.StatusUnauthorized,
					"message":     "invalid token",
					"error":       "invalid token",
				})
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
					"status_code": fiber.StatusServiceUnavailable,
					"message":     "authentication service busy",
					"error":       "authentication service busy",
				})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status_code": fiber.StatusInternalServerError,
				"message":     "authentication failed",
				"error":       "authentication failed",
			})
		}

		if !user.IsActive {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"status_code": fiber.StatusForbidden,
				"message":     "user account is disabled",
				"error":       "user account is disabled",
			})
		}

		if claims.IssuedAt == nil || claims.IssuedAt.Unix() < user.TokenGeneratedAt.Unix() {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"status_code": fiber.StatusUnauthorized,
				"message":     "token has been revoked, please login again",
				"error":       "token has been revoked, please login again",
			})
		}

		c.Locals("user", user)
		c.Locals("jwt_claims", claims)

		return c.Next()
	}
}
