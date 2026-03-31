package config

import (
	"context"
	"encoding/json"
	"errors"
	"feedprovider/models"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	ws "github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"
)

const (
	socketPingPeriod = 25 * time.Second
	socketWriteWait  = 10 * time.Second
)

type socketClient struct {
	conn      *ws.Conn
	userToken string
	userID    string
	filterSym string
	mu        sync.Mutex
}

type socketJWTClaims struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type SocketHub struct {
	mu                 sync.RWMutex
	clients            map[*socketClient]struct{}
	db                 *gorm.DB
	initialStateGetter func(ctx context.Context, filterSym string) []json.RawMessage
}

func NewSocketHub(db *gorm.DB) *SocketHub {
	return &SocketHub{
		clients: make(map[*socketClient]struct{}),
		db:      db,
	}
}

func (h *SocketHub) SetInitialStateGetter(fn func(ctx context.Context, filterSym string) []json.RawMessage) {
	h.initialStateGetter = fn
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
		defer h.removeClient(client, "")

		if h.initialStateGetter != nil {
			initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer initCancel()
			for _, raw := range h.initialStateGetter(initCtx, "") {
				client.mu.Lock()
				_ = c.SetWriteDeadline(time.Now().Add(socketWriteWait))
				_ = c.WriteMessage(ws.TextMessage, raw)
				client.mu.Unlock()
			}
		}

		done := make(chan struct{})
		defer close(done)

		go func(conn *ws.Conn, writeMu *sync.Mutex, userName string, finished <-chan struct{}) {
			ticker := time.NewTicker(socketPingPeriod)
			defer ticker.Stop()

			for {
				select {
				case <-finished:
					return
				case <-ticker.C:
					writeMu.Lock()
					_ = conn.SetWriteDeadline(time.Now().Add(socketWriteWait))
					err := conn.WriteControl(ws.PingMessage, []byte("ping"), time.Now().Add(socketWriteWait))
					writeMu.Unlock()
					if err != nil {
						log.Printf("websocket heartbeat failed: user=%s err=%v", userName, err)
						_ = conn.Close()
						return
					}
				}
			}
		}(c, &client.mu, user.Username, done)

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				log.Printf("websocket client disconnected: user=%s", user.Username)
				return
			}
		}
	}))
}

func (h *SocketHub) RegisterSingleInstrumentRoutes(app *fiber.App, path string) {
	app.Use(path, func(c *fiber.Ctx) error {
		if ws.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get(path, ws.New(func(c *ws.Conn) {
		token := strings.TrimSpace(c.Query("token"))
		if token == "" {
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusUnauthorized, "error": "token required in query parameter"})
			_ = c.Close()
			return
		}

		user, err := h.resolveUserFromSocketToken(token)
		if err != nil {
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusUnauthorized, "error": "invalid token"})
			_ = c.Close()
			return
		}

		if !user.IsActive {
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusForbidden, "error": "user account is disabled"})
			_ = c.Close()
			return
		}

		if user.IsTokenExpired() {
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusUnauthorized, "error": "token expired", "message": "please refresh your token"})
			_ = c.Close()
			return
		}

		filterSymbol, err := h.resolveFilterSymbol(c.Query("symbol"), c.Query("instrument_token"))
		if err != nil {
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusBadRequest, "error": err.Error()})
			_ = c.Close()
			return
		}

		client := &socketClient{conn: c, userToken: token, userID: user.ID, filterSym: filterSymbol}
		h.addClient(client)
		log.Printf("filtered websocket connected: user=%s symbol=%s", user.Username, filterSymbol)
		defer h.removeClient(client, filterSymbol)

		if h.initialStateGetter != nil {
			initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer initCancel()
			for _, raw := range h.initialStateGetter(initCtx, filterSymbol) {
				client.mu.Lock()
				_ = c.SetWriteDeadline(time.Now().Add(socketWriteWait))
				_ = c.WriteMessage(ws.TextMessage, raw)
				client.mu.Unlock()
			}
		}

		done := make(chan struct{})
		defer close(done)

		go func(conn *ws.Conn, writeMu *sync.Mutex, finished <-chan struct{}) {
			ticker := time.NewTicker(socketPingPeriod)
			defer ticker.Stop()

			for {
				select {
				case <-finished:
					return
				case <-ticker.C:
					writeMu.Lock()
					_ = conn.SetWriteDeadline(time.Now().Add(socketWriteWait))
					err := conn.WriteControl(ws.PingMessage, []byte("ping"), time.Now().Add(socketWriteWait))
					writeMu.Unlock()
					if err != nil {
						_ = conn.Close()
						return
					}
				}
			}
		}(c, &client.mu, done)

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

func (h *SocketHub) BroadcastJSON(payload any) {
	broadcastSymbol := extractSymbolFromPayload(payload)

	h.mu.RLock()
	clients := make([]*socketClient, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	failedClients := make([]*socketClient, 0, len(clients))

	for _, client := range clients {
		if client.filterSym != "" {
			if broadcastSymbol == "" || !strings.EqualFold(client.filterSym, broadcastSymbol) {
				continue
			}
		}

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

func (h *SocketHub) removeClient(client *socketClient, filterSymbol string) {
	h.mu.Lock()
	delete(h.clients, client)
	h.mu.Unlock()
	client.mu.Lock()
	_ = client.conn.Close()
	client.mu.Unlock()
	if filterSymbol != "" {
		log.Printf("filtered websocket disconnected: symbol=%s", filterSymbol)
	}
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

func (h *SocketHub) resolveFilterSymbol(rawSymbol, rawToken string) (string, error) {
	symbol := strings.ToUpper(strings.TrimSpace(rawSymbol))
	instrumentToken := strings.TrimSpace(rawToken)

	if symbol == "" && instrumentToken == "" {
		return "", errors.New("symbol or instrument_token query param is required")
	}

	if symbol != "" {
		return symbol, nil
	}

	parsedToken, err := strconv.ParseInt(instrumentToken, 10, 64)
	if err != nil || parsedToken <= 0 {
		return "", errors.New("instrument_token must be a positive integer")
	}

	var instrument models.Instrument
	err = h.db.Where("instrument_token = ? AND is_deleted = ?", parsedToken, false).First(&instrument).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", errors.New("instrument not found for provided instrument_token")
		}
		return "", errors.New("failed to resolve instrument_token")
	}

	if !instrument.IsActive() {
		return "", errors.New("instrument is inactive")
	}

	resolvedSymbol := strings.ToUpper(strings.TrimSpace(instrument.TradingSymbol))
	if resolvedSymbol == "" {
		return "", errors.New("instrument symbol is empty")
	}

	return resolvedSymbol, nil
}

func extractSymbolFromPayload(payload any) string {
	switch value := payload.(type) {
	case map[string]any:
		if symbol, ok := value["symbol"].(string); ok {
			return strings.TrimSpace(symbol)
		}
	case map[string]string:
		return strings.TrimSpace(value["symbol"])
	}

	v := reflect.ValueOf(payload)
	if !v.IsValid() {
		return ""
	}

	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return ""
	}

	field := v.FieldByName("Symbol")
	if !field.IsValid() || field.Kind() != reflect.String {
		return ""
	}

	return strings.TrimSpace(field.String())
}
