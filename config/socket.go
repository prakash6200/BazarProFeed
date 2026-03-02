package config

import (
	"sync"
	"time"

	ws "github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

type socketClient struct {
	conn *ws.Conn
	mu   sync.Mutex
}

type SocketHub struct {
	mu      sync.RWMutex
	clients map[*socketClient]struct{}
}

func NewSocketHub() *SocketHub {
	return &SocketHub{
		clients: make(map[*socketClient]struct{}),
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
		client := &socketClient{conn: c}
		h.addClient(client)
		defer h.removeClient(client)

		for {
			if _, _, err := c.ReadMessage(); err != nil {
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
