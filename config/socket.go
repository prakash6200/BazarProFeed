package config

import (
	"errors"
	"feedprovider/models"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	ws "github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"
)

type socketClient struct {
	conn      *ws.Conn
	userToken string
	userID    string
	mu        sync.Mutex
}

type socketJWTClaims struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type SocketHub struct {
	mu      sync.RWMutex
	clients map[*socketClient]struct{}
	db      *gorm.DB
}

func NewSocketHub(db *gorm.DB) *SocketHub {
	return &SocketHub{
		clients: make(map[*socketClient]struct{}),
		db:      db,
	}
}

func (h *SocketHub) RegisterRoutes(app *fiber.App, path string) {
	app.Use(path, func(c *fiber.Ctx) error {
		if ws.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get(path, ws.New(func(c *ws.Conn) {

		token := c.Query("token")
		if token == "" {
			log.Println("websocket connection rejected: missing token")
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusUnauthorized, "error": "token required in query parameter"})
			_ = c.Close()
			return
		}

		user, err := h.resolveUserFromSocketToken(token)
		if err != nil {
			log.Printf("websocket connection rejected: invalid token")
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusUnauthorized, "error": "invalid token"})
			_ = c.Close()
			return
		}

		if !user.IsActive {
			log.Printf("websocket connection rejected: user %s is disabled", user.Username)
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusForbidden, "error": "user account is disabled"})
			_ = c.Close()
			return
		}

		if user.IsTokenExpired() {
			log.Printf("websocket connection rejected: token expired for user %s", user.Username)
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusUnauthorized, "error": "token expired", "message": "please refresh your token"})
			_ = c.Close()
			return
		}

		client := &socketClient{conn: c, userToken: token, userID: user.ID}
		h.addClient(client)
		log.Printf("websocket client connected: user=%s, token=%s", user.Username, maskToken(token))
		defer h.removeClient(client)

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				log.Printf("websocket client disconnected: user=%s", user.Username)
				return
			}
		}
	}))
}

func (h *SocketHub) BroadcastJSON(payload any) {
	h.mu.RLock()
	clients := make([]*socketClient, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	failedClients := make([]*socketClient, 0, len(clients))

	for _, client := range clients {
		client.mu.Lock()
		_ = client.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		err := client.conn.WriteJSON(payload)
		client.mu.Unlock()
		if err != nil {
			failedClients = append(failedClients, client)
		}
	}

	h.removeClients(failedClients)
}

func (h *SocketHub) CloseAll() {
	h.mu.Lock()
	clients := make([]*socketClient, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
		delete(h.clients, client)
	}
	h.mu.Unlock()

	for _, client := range clients {
		client.mu.Lock()
		_ = client.conn.Close()
		client.mu.Unlock()
	}
}

func (h *SocketHub) addClient(client *socketClient) {
	h.mu.Lock()
	h.clients[client] = struct{}{}
	h.mu.Unlock()
}

func (h *SocketHub) removeClient(client *socketClient) {
	h.mu.Lock()
	delete(h.clients, client)
	h.mu.Unlock()
	client.mu.Lock()
	_ = client.conn.Close()
	client.mu.Unlock()
}

func (h *SocketHub) removeClients(clients []*socketClient) {
	if len(clients) == 0 {
		return
	}

	h.mu.Lock()
	for _, client := range clients {
		delete(h.clients, client)
	}
	h.mu.Unlock()

	for _, client := range clients {
		client.mu.Lock()
		_ = client.conn.Close()
		client.mu.Unlock()
	}
}

func (h *SocketHub) CloseUserConnections(token string) {
	h.mu.Lock()
	clientsToClose := make([]*socketClient, 0)
	for client := range h.clients {
		if client.userToken == token || client.userID == token {
			clientsToClose = append(clientsToClose, client)
			delete(h.clients, client)
		}
	}
	h.mu.Unlock()

	for _, client := range clientsToClose {
		client.mu.Lock()
		_ = client.conn.Close()
		client.mu.Unlock()
	}

	if len(clientsToClose) > 0 {
		log.Printf("closed %d connections for token %s", len(clientsToClose), maskToken(token))
	}
}

func maskToken(token string) string {
	if len(token) <= 10 {
		return token
	}
	return token[:10] + "..."
}

func (h *SocketHub) resolveUserFromSocketToken(token string) (*models.User, error) {
	if user, err := h.resolveUserFromJWT(token); err == nil {
		return user, nil
	}

	return models.GetUserByToken(h.db, token)
}

func (h *SocketHub) resolveUserFromJWT(token string) (*models.User, error) {
	secret := strings.TrimSpace(os.Getenv("JWT_SECRET"))
	if secret == "" {
		return nil, errors.New("jwt secret not configured")
	}

	claims := &socketJWTClaims{}
	parsedToken, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return []byte(secret), nil
	})
	if err != nil || !parsedToken.Valid || claims.UserID == "" {
		return nil, errors.New("invalid jwt token")
	}

	return models.GetUserByID(h.db, claims.UserID)
}

func (h *SocketHub) GetClientCount() int {
	h.mu.RLock()
	count := len(h.clients)
	h.mu.RUnlock()
	return count
}
