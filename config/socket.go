package config

import (
	"context"
	"encoding/json"
	"errors"
	"feedprovider/models"
	"log"
	"os"
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
	socketPingPeriod     = 25 * time.Second
	socketWriteWait      = 10 * time.Second
	socketReadWait       = 60 * time.Second
	socketMaxBulkFilters = 1000
	clientSendBuffer     = 256

	// Hard cap on concurrent WebSocket clients PER HUB. Each client
	// holds two goroutines and one FD; without a cap a misbehaving or
	// abusive peer could spawn connections until the process runs out
	// of file descriptors — at which point accept() starts failing
	// silently and the port "stays open" but nothing responds.
	socketMaxClients = 5000
)

type socketClient struct {
	conn       *ws.Conn
	userToken  string
	userID     string
	filterSyms map[string]struct{}
	mu         sync.Mutex
	sendCh     chan []byte
	done       chan struct{}
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

func newSocketClient(conn *ws.Conn, token, userID string, filterSyms map[string]struct{}) *socketClient {
	return &socketClient{
		conn:       conn,
		userToken:  token,
		userID:     userID,
		filterSyms: filterSyms,
		sendCh:     make(chan []byte, clientSendBuffer),
		done:       make(chan struct{}),
	}
}

func (c *socketClient) writePump() {
	ticker := time.NewTicker(socketPingPeriod)
	defer ticker.Stop()
	defer func() {
		// Close the connection so the ReadMessage loop in the handler unblocks.
		c.mu.Lock()
		_ = c.conn.Close()
		c.mu.Unlock()
	}()

	for {
		select {
		case msg, ok := <-c.sendCh:
			if !ok {
				return
			}
			c.mu.Lock()
			_ = c.conn.SetWriteDeadline(time.Now().Add(socketWriteWait))
			err := c.conn.WriteMessage(ws.TextMessage, msg)
			c.mu.Unlock()
			if err != nil {
				return
			}
		case <-ticker.C:
			c.mu.Lock()
			_ = c.conn.SetWriteDeadline(time.Now().Add(socketWriteWait))
			err := c.conn.WriteControl(ws.PingMessage, []byte("ping"), time.Now().Add(socketWriteWait))
			c.mu.Unlock()
			if err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

// close signals the writePump to exit and closes the underlying
// connection. It MUST NOT acquire c.mu — writePump holds that lock
// while executing WriteMessage (up to socketWriteWait = 10s), and a
// caller waiting on c.mu here would stall the goroutine that invoked
// close (typically the broadcast loop via removeClient).
//
// conn.Close is safe for concurrent use with writes in
// fasthttp/websocket; the ongoing WriteMessage will return an error
// and the writePump will exit through its defer.
func (c *socketClient) close() {
	select {
	case <-c.done:
		return // already closed
	default:
		close(c.done)
	}
	_ = c.conn.Close()
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

		client := newSocketClient(c, token, user.ID, nil)
		if !h.addClient(client) {
			log.Printf("websocket connection rejected: hub at capacity (max=%d)", socketMaxClients)
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusServiceUnavailable, "error": "server at capacity, try again later"})
			_ = c.Close()
			return
		}
		log.Printf("websocket client connected: user=%s, token=%s", user.Username, maskToken(token))
		defer h.removeClient(client)

		if h.initialStateGetter != nil {
			initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Second)
			for _, raw := range h.initialStateGetter(initCtx, "") {
				select {
				case client.sendCh <- raw:
				default:
				}
			}
			initCancel()
		}

		_ = c.SetReadDeadline(time.Now().Add(socketReadWait))
		c.SetPongHandler(func(string) error {
			return c.SetReadDeadline(time.Now().Add(socketReadWait))
		})

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

		client := newSocketClient(c, token, user.ID, makeSymbolSet([]string{filterSymbol}))
		if !h.addClient(client) {
			log.Printf("filtered websocket rejected: hub at capacity (max=%d)", socketMaxClients)
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusServiceUnavailable, "error": "server at capacity, try again later"})
			_ = c.Close()
			return
		}
		log.Printf("filtered websocket connected: user=%s symbol=%s", user.Username, filterSymbol)
		defer h.removeClient(client)

		if h.initialStateGetter != nil {
			initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Second)
			for _, raw := range h.initialStateGetter(initCtx, filterSymbol) {
				select {
				case client.sendCh <- raw:
				default:
				}
			}
			initCancel()
		}

		_ = c.SetReadDeadline(time.Now().Add(socketReadWait))
		c.SetPongHandler(func(string) error {
			return c.SetReadDeadline(time.Now().Add(socketReadWait))
		})

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

func (h *SocketHub) RegisterBulkInstrumentRoutes(app *fiber.App, path string) {
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

		filterSymbols, err := h.resolveBulkFilterSymbols(c.Query("symbols"), c.Query("instrument_tokens"))
		if err != nil {
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusBadRequest, "error": err.Error()})
			_ = c.Close()
			return
		}

		client := newSocketClient(c, token, user.ID, makeSymbolSet(filterSymbols))
		if !h.addClient(client) {
			log.Printf("bulk filtered websocket rejected: hub at capacity (max=%d)", socketMaxClients)
			_ = c.WriteJSON(fiber.Map{"status_code": fiber.StatusServiceUnavailable, "error": "server at capacity, try again later"})
			_ = c.Close()
			return
		}
		log.Printf("bulk filtered websocket connected: user=%s symbols=%d", user.Username, len(filterSymbols))
		defer h.removeClient(client)

		if h.initialStateGetter != nil {
			initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Second)
			for _, symbol := range filterSymbols {
				for _, raw := range h.initialStateGetter(initCtx, symbol) {
					select {
					case client.sendCh <- raw:
					default:
					}
				}
			}
			initCancel()
		}

		_ = c.SetReadDeadline(time.Now().Add(socketReadWait))
		c.SetPongHandler(func(string) error {
			return c.SetReadDeadline(time.Now().Add(socketReadWait))
		})

		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

// BroadcastRaw sends an already-marshalled payload to every connected
// client. Symbol is used for filter matching and MUST be uppercase.
// All client sends are non-blocking (slow clients drop messages) so
// this method runs in bounded time regardless of client count.
func (h *SocketHub) BroadcastRaw(symbol string, data []byte) {
	if len(data) == 0 {
		return
	}
	broadcastSymbol := strings.ToUpper(strings.TrimSpace(symbol))

	h.mu.RLock()
	// Avoid allocating a fresh slice each broadcast by doing the send
	// under a read-lock. Sends are non-blocking, so holding RLock for
	// the duration of the fan-out is O(numClients) with no contention
	// risk (writers to the map are only Register/Close paths).
	for client := range h.clients {
		if len(client.filterSyms) > 0 {
			if broadcastSymbol == "" || !client.matchesSymbol(broadcastSymbol) {
				continue
			}
		}
		select {
		case client.sendCh <- data:
		default:
			// slow client — drop message instead of blocking the broadcast loop
		}
	}
	h.mu.RUnlock()
}

// BroadcastJSON is retained for compatibility with non-tick callers
// (e.g. admin notifications). It marshals once and delegates to
// BroadcastRaw. Do NOT use this on the hot tick path.
func (h *SocketHub) BroadcastJSON(payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	symbol := ""
	switch v := payload.(type) {
	case map[string]any:
		if s, ok := v["symbol"].(string); ok {
			symbol = s
		}
	case map[string]string:
		symbol = v["symbol"]
	}

	h.BroadcastRaw(symbol, data)
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
		client.close()
	}
}

// addClient registers a new WebSocket client. Returns false if the hub
// is at capacity — callers MUST close the underlying connection in
// that case to avoid leaking FDs.
func (h *SocketHub) addClient(client *socketClient) bool {
	h.mu.Lock()
	if len(h.clients) >= socketMaxClients {
		h.mu.Unlock()
		return false
	}
	h.clients[client] = struct{}{}
	h.mu.Unlock()
	go client.writePump()
	return true
}

func (h *SocketHub) removeClient(client *socketClient) {
	h.mu.Lock()
	delete(h.clients, client)
	h.mu.Unlock()
	client.close()
	if len(client.filterSyms) > 0 {
		log.Printf("filtered websocket disconnected: symbols=%d", len(client.filterSyms))
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
		client.close()
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
		client.close()
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

	// Short timeout: if the DB pool is saturated we'd rather reject
	// the handshake than keep a goroutine waiting indefinitely for a
	// connection. The handshake timeout also bounds this upstream in
	// Fiber/fasthttp, but we enforce it explicitly here.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return models.GetUserByToken(h.db.WithContext(ctx), token)
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return models.GetUserByID(h.db.WithContext(ctx), claims.UserID)
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
	resolvedByToken, err := h.resolveSymbolsByInstrumentTokens([]int64{parsedToken})
	if err != nil {
		return "", err
	}
	resolvedSymbol, ok := resolvedByToken[parsedToken]
	if !ok {
		return "", errors.New("instrument not found for provided instrument_token")
	}
	return resolvedSymbol, nil
}

func (h *SocketHub) resolveBulkFilterSymbols(rawSymbols, rawTokens string) ([]string, error) {
	rawSymbolItems := splitCSVValues(rawSymbols)
	rawTokenItems := splitCSVValues(rawTokens)

	if len(rawSymbolItems) == 0 && len(rawTokenItems) == 0 {
		return nil, errors.New("symbols or instrument_tokens query param is required")
	}

	if len(rawSymbolItems)+len(rawTokenItems) > socketMaxBulkFilters {
		return nil, errors.New("too many bulk filters requested, max " + strconv.Itoa(socketMaxBulkFilters))
	}

	set := make(map[string]struct{})
	result := make([]string, 0, len(rawSymbolItems)+len(rawTokenItems))

	for _, symbol := range rawSymbolItems {
		resolved, err := h.resolveFilterSymbol(symbol, "")
		if err != nil {
			return nil, err
		}
		if _, ok := set[resolved]; !ok {
			set[resolved] = struct{}{}
			result = append(result, resolved)
		}
	}

	parsedTokens := make([]int64, 0, len(rawTokenItems))
	for _, token := range rawTokenItems {
		parsedToken, err := strconv.ParseInt(strings.TrimSpace(token), 10, 64)
		if err != nil || parsedToken <= 0 {
			return nil, errors.New("instrument_token must be a positive integer")
		}
		parsedTokens = append(parsedTokens, parsedToken)
	}

	resolvedByToken, err := h.resolveSymbolsByInstrumentTokens(parsedTokens)
	if err != nil {
		return nil, err
	}

	for _, parsedToken := range parsedTokens {
		resolved, ok := resolvedByToken[parsedToken]
		if !ok {
			return nil, errors.New("instrument not found for provided instrument_token")
		}

		if _, ok := set[resolved]; !ok {
			set[resolved] = struct{}{}
			result = append(result, resolved)
		}
	}

	if len(result) == 0 {
		return nil, errors.New("no valid symbols resolved for bulk subscribe")
	}

	return result, nil
}

func (h *SocketHub) resolveSymbolsByInstrumentTokens(tokens []int64) (map[int64]string, error) {
	if len(tokens) == 0 {
		return map[int64]string{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dbCtx := h.db.WithContext(ctx)

	resolved := make(map[int64]string, len(tokens))

	var instruments []models.Instrument
	if err := dbCtx.
		Select("instrument_token", "trading_symbol", "status", "is_deleted").
		Where("instrument_token IN ? AND is_deleted = ?", tokens, false).
		Find(&instruments).Error; err != nil {
		return nil, errors.New("failed to resolve instrument_token")
	}

	for _, instrument := range instruments {
		if !instrument.IsActive() {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(instrument.TradingSymbol))
		if symbol == "" {
			continue
		}
		resolved[instrument.InstrumentToken] = symbol
	}

	unresolved := make([]int64, 0)
	for _, token := range tokens {
		if _, ok := resolved[token]; !ok {
			unresolved = append(unresolved, token)
		}
	}

	if len(unresolved) == 0 {
		return resolved, nil
	}

	var globalInstruments []models.GlobalInstrument
	if err := dbCtx.
		Select("instrument_token", "symbol", "trading_symbol", "status", "is_deleted").
		Where("instrument_token IN ? AND is_deleted = ?", unresolved, false).
		Find(&globalInstruments).Error; err != nil {
		return nil, errors.New("failed to resolve instrument_token")
	}

	for _, instrument := range globalInstruments {
		if !instrument.IsActive() {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(instrument.Symbol))
		if symbol == "" {
			symbol = strings.ToUpper(strings.TrimSpace(instrument.TradingSymbol))
		}
		if symbol == "" {
			continue
		}
		resolved[instrument.InstrumentToken] = symbol
	}

	return resolved, nil
}

func splitCSVValues(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func makeSymbolSet(symbols []string) map[string]struct{} {
	set := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		normalized := strings.ToUpper(strings.TrimSpace(symbol))
		if normalized != "" {
			set[normalized] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}

func (c *socketClient) matchesSymbol(symbol string) bool {
	if len(c.filterSyms) == 0 {
		return true
	}
	normalized := strings.ToUpper(strings.TrimSpace(symbol))
	_, ok := c.filterSyms[normalized]
	return ok
}
