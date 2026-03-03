package config

import (
	"feedprovider/models"
	"log"
	"sync"
	"time"

	ws "github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type socketClient struct {
	conn      *ws.Conn
	userToken string
	mu        sync.Mutex
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
		// Get token from query parameter
		token := c.Query("token")
		if token == "" {
			log.Println("websocket connection rejected: missing token")
			_ = c.WriteJSON(fiber.Map{"error": "token required in query parameter"})
			_ = c.Close()
			return
		}

		// Validate token
		user, err := models.GetUserByToken(h.db, token)
		if err != nil {
			log.Printf("websocket connection rejected: invalid token")
			_ = c.WriteJSON(fiber.Map{"error": "invalid token"})
			_ = c.Close()
			return
		}

		// Check if user is active
		if !user.IsActive {
			log.Printf("websocket connection rejected: user %s is disabled", user.Username)
			_ = c.WriteJSON(fiber.Map{"error": "user account is disabled"})
			_ = c.Close()
			return
		}

		// Check if token is expired
		if user.IsTokenExpired() {
			log.Printf("websocket connection rejected: token expired for user %s", user.Username)
			_ = c.WriteJSON(fiber.Map{"error": "token expired", "message": "please refresh your token"})
			_ = c.Close()
			return
		}

		// Create authenticated client
		client := &socketClient{conn: c, userToken: token}
		h.addClient(client)
		log.Printf("websocket client connected: user=%s, token=%s...", user.Username, token[:10])
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

// CloseUserConnections closes all connections for a specific user token
func (h *SocketHub) CloseUserConnections(token string) {
	h.mu.Lock()
	clientsToClose := make([]*socketClient, 0)
	for client := range h.clients {
		if client.userToken == token {
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
		log.Printf("closed %d connections for token %s...", len(clientsToClose), token[:10])
	}
}

// GetClientCount returns the current number of connected clients
func (h *SocketHub) GetClientCount() int {
	h.mu.RLock()
	count := len(h.clients)
	h.mu.RUnlock()
	return count
}
