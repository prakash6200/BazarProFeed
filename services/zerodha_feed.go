package services

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"feedprovider/config"

	"github.com/gorilla/websocket"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

const (
	modeQuote TickMode = "quote"
	modeFull  TickMode = "full"

	connectionStateDisconnected ConnectionState = "disconnected"
	connectionStateConnecting   ConnectionState = "connecting"
	connectionStateConnected    ConnectionState = "connected"
	connectionStateReconnecting ConnectionState = "reconnecting"

	heartbeatReadTimeout = 60 * time.Second
	heartbeatPingPeriod  = 20 * time.Second

	maxReconnectBackoff = 30 * time.Second
	spikeAlertPercent   = 20.0

	packetMinLength     = 8
	packetQuoteOHLCMin  = 28
	packetQuoteOHLCMax  = 44
	packetFullMin       = 44
	packetFullWithOI    = 52
	packetFullWithDepth = 184

	packetOffsetToken = 0
	packetOffsetLTP   = 4

	packetOffsetOpen  = 28
	packetOffsetHigh  = 32
	packetOffsetLow   = 36
	packetOffsetClose = 40

	packetOffsetTBQ = 20
	packetOffsetTSQ = 24
	packetOffsetOI  = 48

	depthStartOffset = 64
	depthLevelSize   = 12
	depthLevels      = 5
)

var (
	errZerodhaAuthFailed  = errors.New("zerodha authentication failed")
	errAccessTokenExpired = errors.New("zerodha access token expired")
)

type ConnectionState string

type TickMode string

type instrumentMeta struct {
	Mode     TickMode
	Exchange string
	Symbol   string
}

type kiteErrorResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"error_type"`
	Message   string `json:"message"`
}

type subscriptionRegistry struct {
	mu    sync.RWMutex
	items map[int64]instrumentMeta
}

func newSubscriptionRegistry() *subscriptionRegistry {
	return &subscriptionRegistry{items: make(map[int64]instrumentMeta)}
}

func (r *subscriptionRegistry) Add(token int64, meta instrumentMeta) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[token]; ok {
		return false
	}
	r.items[token] = meta
	return true
}

func (r *subscriptionRegistry) Remove(token int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[token]; !ok {
		return false
	}
	delete(r.items, token)
	return true
}

func (r *subscriptionRegistry) SwitchMode(token int64, mode TickMode) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.items[token]
	if !ok {
		return false
	}
	meta.Mode = mode
	r.items[token] = meta
	return true
}

func (r *subscriptionRegistry) Get(token int64) (instrumentMeta, bool) {
	r.mu.RLock()
	meta, ok := r.items[token]
	r.mu.RUnlock()
	return meta, ok
}

func (r *subscriptionRegistry) SnapshotTokens() []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]int64, 0, len(r.items))
	for token := range r.items {
		result = append(result, token)
	}
	return result
}

func (r *subscriptionRegistry) SnapshotByMode(mode TickMode) []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]int64, 0, len(r.items))
	for token, meta := range r.items {
		if meta.Mode == mode {
			result = append(result, token)
		}
	}
	return result
}

type ZerodhaFeedService struct {
	cfg         config.ZerodhaConfig
	tickHub     *TickHub
	marketState *MarketStateManager
	registry    *subscriptionRegistry
	httpClient  *http.Client

	running atomic.Bool

	stateMu sync.RWMutex
	state   ConnectionState

	connMu sync.RWMutex
	conn   *websocket.Conn

	writeMu sync.Mutex
}

func NewZerodhaFeedService(cfg config.ZerodhaConfig, tickHub *TickHub) *ZerodhaFeedService {
	service := &ZerodhaFeedService{
		cfg:         cfg,
		tickHub:     tickHub,
		marketState: NewMarketStateManager(),
		registry:    newSubscriptionRegistry(),
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		state:       connectionStateDisconnected,
	}
	service.seedRegistryFromConfig()
	return service
}

func (s *ZerodhaFeedService) Start(ctx context.Context) {
	if !s.running.CompareAndSwap(false, true) {
		log.Println("zerodha feed already running, skipping duplicate start")
		return
	}

	if strings.TrimSpace(s.cfg.APIKey) == "" {
		log.Println("zerodha feed skipped: set ZERODHA_API_KEY in .env")
		s.running.Store(false)
		return
	}

	if s.shouldExchangeRequestToken() {
		if err := s.ensureAccessTokenFromRequestToken(); err != nil {
			log.Printf("zerodha feed skipped: %v", err)
			s.running.Store(false)
			return
		}
	}

	if strings.TrimSpace(s.cfg.AccessToken) == "" {
		log.Println("zerodha feed skipped: set ZERODHA_ACCESS_TOKEN or ZERODHA_REQUEST_TOKEN in .env")
		s.running.Store(false)
		return
	}

	if len(s.registry.SnapshotTokens()) == 0 {
		log.Println("zerodha feed skipped: set ZERODHA_INDEX_INSTRUMENTS and/or ZERODHA_MCX_INSTRUMENTS (fallback: ZERODHA_INSTRUMENTS)")
		s.running.Store(false)
		return
	}

	if err := s.validateAccessToken(); err != nil {
		if s.shouldExchangeRequestToken() {
			if exchangeErr := s.ensureAccessTokenFromRequestToken(); exchangeErr == nil {
				if revalidateErr := s.validateAccessToken(); revalidateErr == nil {
					go s.runReconnectLoop(ctx)
					return
				}
			}
		}
		s.handleAuthFailure(err)
		s.running.Store(false)
		return
	}

	go s.runReconnectLoop(ctx)
}

func (s *ZerodhaFeedService) shouldExchangeRequestToken() bool {
	requestToken := extractRequestToken(strings.TrimSpace(s.cfg.RequestToken))
	if requestToken == "" {
		return false
	}

	accessToken := strings.TrimSpace(s.cfg.AccessToken)
	if accessToken == "" {
		return true
	}

	if accessToken == requestToken {
		return true
	}

	if strings.Contains(accessToken, "request_token=") {
		return true
	}

	return false
}

func (s *ZerodhaFeedService) ensureAccessTokenFromRequestToken() error {
	if strings.TrimSpace(s.cfg.APIKey) == "" {
		return errors.New("set ZERODHA_API_KEY in .env")
	}

	requestToken := extractRequestToken(strings.TrimSpace(s.cfg.RequestToken))
	if requestToken == "" {
		return fmt.Errorf("set ZERODHA_REQUEST_TOKEN in .env (login URL: https://kite.zerodha.com/connect/login?v=3&api_key=%s)", s.cfg.APIKey)
	}

	if strings.TrimSpace(s.cfg.APISecret) == "" {
		return errors.New("set ZERODHA_API_SECRET to exchange ZERODHA_REQUEST_TOKEN")
	}

	accessToken, err := s.exchangeRequestToken(requestToken)
	if err != nil {
		return err
	}

	s.cfg.AccessToken = accessToken
	log.Println("zerodha access token generated from request token for current runtime")
	log.Printf("set this in .env for reuse: ZERODHA_ACCESS_TOKEN=%s", accessToken)
	return nil
}

func extractRequestToken(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}

	if !strings.Contains(value, "request_token=") {
		return value
	}

	parsedURL, err := url.Parse(value)
	if err != nil {
		return value
	}

	queryToken := strings.TrimSpace(parsedURL.Query().Get("request_token"))
	if queryToken != "" {
		return queryToken
	}

	return value
}

func (s *ZerodhaFeedService) exchangeRequestToken(requestToken string) (string, error) {
	kc := kiteconnect.New(strings.TrimSpace(s.cfg.APIKey))
	session, err := kc.GenerateSession(requestToken, strings.TrimSpace(s.cfg.APISecret))
	if err != nil {
		return "", fmt.Errorf("%w: %v", errZerodhaAuthFailed, err)
	}

	accessToken := strings.TrimSpace(session.AccessToken)
	if accessToken == "" {
		return "", fmt.Errorf("%w: empty access token in session response", errZerodhaAuthFailed)
	}

	return accessToken, nil
}

func (s *ZerodhaFeedService) ConnectionState() ConnectionState {
	s.stateMu.RLock()
	state := s.state
	s.stateMu.RUnlock()
	return state
}

func (s *ZerodhaFeedService) AddInstrument(token int64, mode TickMode, exchange, symbol string) error {
	if token <= 0 {
		return fmt.Errorf("invalid token: %d", token)
	}
	if mode != modeQuote && mode != modeFull {
		return fmt.Errorf("invalid mode: %s", mode)
	}
	exchange, symbol = sanitizeInstrumentIdentity(token, exchange, symbol)

	added := s.registry.Add(token, instrumentMeta{Mode: mode, Exchange: exchange, Symbol: symbol})
	if !added {
		return nil
	}

	conn := s.getConn()
	if conn == nil {
		return nil
	}

	if err := s.writeJSON(conn, map[string]any{"a": "subscribe", "v": []int64{token}}); err != nil {
		return err
	}
	if err := s.writeJSON(conn, map[string]any{"a": "mode", "v": []any{string(mode), []int64{token}}}); err != nil {
		return err
	}
	return nil
}

func (s *ZerodhaFeedService) RemoveInstrument(token int64) error {
	removed := s.registry.Remove(token)
	if !removed {
		return nil
	}

	conn := s.getConn()
	if conn == nil {
		return nil
	}
	return s.writeJSON(conn, map[string]any{"a": "unsubscribe", "v": []int64{token}})
}

func (s *ZerodhaFeedService) SwitchMode(token int64, mode TickMode) error {
	if mode != modeQuote && mode != modeFull {
		return fmt.Errorf("invalid mode: %s", mode)
	}
	ok := s.registry.SwitchMode(token, mode)
	if !ok {
		return fmt.Errorf("instrument not found: %d", token)
	}

	conn := s.getConn()
	if conn == nil {
		return nil
	}
	return s.writeJSON(conn, map[string]any{"a": "mode", "v": []any{string(mode), []int64{token}}})
}

func (s *ZerodhaFeedService) GetLatestState(symbol string) (NormalizedTick, bool) {
	return s.marketState.Get(symbol)
}

func (s *ZerodhaFeedService) runReconnectLoop(ctx context.Context) {
	defer func() {
		s.setConnectionState(connectionStateDisconnected)
		s.running.Store(false)
	}()

	backoff := 1 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			return
		}

		err := s.connectAndRead(ctx)
		if err == nil {
			backoff = 1 * time.Second
			continue
		}
		if ctx.Err() != nil {
			return
		}

		if isAuthFailure(err) {
			s.handleAuthFailure(err)
			return
		}

		s.setConnectionState(connectionStateReconnecting)
		log.Printf("zerodha feed reconnecting in %s due to error: %v", backoff, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxReconnectBackoff {
			backoff = maxReconnectBackoff
		}
	}
}

func (s *ZerodhaFeedService) connectAndRead(ctx context.Context) error {
	wsURL, err := url.Parse(s.cfg.WSEndpoint)
	if err != nil {
		return err
	}

	query := wsURL.Query()
	query.Set("api_key", s.cfg.APIKey)
	query.Set("access_token", s.cfg.AccessToken)
	wsURL.RawQuery = query.Encode()

	s.setConnectionState(connectionStateConnecting)
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.Dial(wsURL.String(), nil)
	if err != nil {
		return wrapDialError(err, resp)
	}
	defer conn.Close()

	s.setConn(conn)
	defer s.clearConn(conn)

	conn.SetReadLimit(2 * 1024 * 1024)
	_ = conn.SetReadDeadline(time.Now().Add(heartbeatReadTimeout))
	conn.SetPongHandler(func(_ string) error {
		return conn.SetReadDeadline(time.Now().Add(heartbeatReadTimeout))
	})

	if err := s.subscribeAll(conn); err != nil {
		return err
	}

	s.setConnectionState(connectionStateConnected)
	subscribers := 0
	if s.tickHub != nil {
		subscribers = s.tickHub.SubscribersCount()
	}
	log.Printf("zerodha websocket connected, subscribers=%d", subscribers)

	pingErrCh := make(chan error, 1)
	go s.startHeartbeat(ctx, conn, pingErrCh)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		select {
		case pingErr := <-pingErrCh:
			return pingErr
		default:
		}

		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(heartbeatReadTimeout))

		switch messageType {
		case websocket.BinaryMessage:
			s.parseAndPublishTicks(payload)
		case websocket.TextMessage:
			text := strings.ToLower(strings.TrimSpace(string(payload)))
			if authErr := parseAuthErrorFromTextMessage(text); authErr != nil {
				return authErr
			}
		default:
			continue
		}
	}
}

func (s *ZerodhaFeedService) startHeartbeat(ctx context.Context, conn *websocket.Conn, errCh chan<- error) {
	ticker := time.NewTicker(heartbeatPingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
			s.writeMu.Unlock()
			if err != nil {
				select {
				case errCh <- fmt.Errorf("heartbeat ping failed: %w", err):
				default:
				}
				return
			}
		}
	}
}

func (s *ZerodhaFeedService) subscribeAll(conn *websocket.Conn) error {
	tokens := s.registry.SnapshotTokens()
	if len(tokens) == 0 {
		return fmt.Errorf("no instruments configured")
	}
	if err := s.writeJSON(conn, map[string]any{"a": "subscribe", "v": tokens}); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	quoteTokens := s.registry.SnapshotByMode(modeQuote)
	if len(quoteTokens) > 0 {
		if err := s.writeJSON(conn, map[string]any{"a": "mode", "v": []any{string(modeQuote), quoteTokens}}); err != nil {
			return fmt.Errorf("set quote mode failed: %w", err)
		}
	}

	fullTokens := s.registry.SnapshotByMode(modeFull)
	if len(fullTokens) > 0 {
		if err := s.writeJSON(conn, map[string]any{"a": "mode", "v": []any{string(modeFull), fullTokens}}); err != nil {
			return fmt.Errorf("set full mode failed: %w", err)
		}
	}

	return nil
}

func (s *ZerodhaFeedService) validateAccessToken() error {
	kc := kiteconnect.New(strings.TrimSpace(s.cfg.APIKey))
	kc.SetAccessToken(strings.TrimSpace(s.cfg.AccessToken))

	_, err := kc.GetUserProfile()
	if err == nil {
		return nil
	}

	message := strings.TrimSpace(err.Error())
	if isExpiredTokenMessage(message) {
		return fmt.Errorf("%w: %s", errAccessTokenExpired, message)
	}

	if message == "" {
		return fmt.Errorf("%w: token validation failed", errZerodhaAuthFailed)
	}

	return fmt.Errorf("%w: %s", errZerodhaAuthFailed, message)
}

func wrapDialError(err error, resp *http.Response) error {
	if resp == nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if isAuthStatus(resp.StatusCode) {
		message := parseKiteErrorMessage(body)
		if isExpiredTokenMessage(message) {
			return fmt.Errorf("%w: %s", errAccessTokenExpired, message)
		}
		if message == "" {
			return fmt.Errorf("%w: status=%d", errZerodhaAuthFailed, resp.StatusCode)
		}
		return fmt.Errorf("%w: %s", errZerodhaAuthFailed, message)
	}

	return fmt.Errorf("dial failed with status=%d: %w", resp.StatusCode, err)
}

func isAuthStatus(statusCode int) bool {
	return statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden
}

func parseKiteErrorMessage(body []byte) string {
	if len(body) == 0 {
		return ""
	}

	var payload kiteErrorResponse
	if err := json.Unmarshal(body, &payload); err == nil {
		if strings.TrimSpace(payload.Message) != "" {
			return payload.Message
		}
		if strings.TrimSpace(payload.ErrorType) != "" {
			return payload.ErrorType
		}
	}

	return strings.TrimSpace(string(body))
}

func isExpiredTokenMessage(message string) bool {
	value := strings.ToLower(strings.TrimSpace(message))
	if value == "" {
		return false
	}

	return strings.Contains(value, "expired") || strings.Contains(value, "invalid session") || strings.Contains(value, "tokenexception")
}

func isAuthFailure(err error) bool {
	return errors.Is(err, errAccessTokenExpired) || errors.Is(err, errZerodhaAuthFailed)
}

func (s *ZerodhaFeedService) handleAuthFailure(err error) {
	switch {
	case errors.Is(err, errAccessTokenExpired):
		log.Printf("zerodha auth failure: access token expired. regenerate token and update ZERODHA_ACCESS_TOKEN. details=%v", err)
	case errors.Is(err, errZerodhaAuthFailed):
		log.Printf("zerodha auth failure: verify ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN. details=%v", err)
	default:
		log.Printf("zerodha auth failure: %v", err)
	}
}

func (s *ZerodhaFeedService) writeJSON(conn *websocket.Conn, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, data)
}

func parseInstrumentIDs(raw string) []int64 {
	parts := strings.Split(raw, ",")
	seen := make(map[int64]struct{}, len(parts))
	result := make([]int64, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Printf("invalid instrument token skipped: %s", value)
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func (s *ZerodhaFeedService) parseAndPublishTicks(payload []byte) {
	if len(payload) == 1 {
		return
	}

	if len(payload) < 2 {
		log.Printf("zerodha malformed payload: length=%d", len(payload))
		return
	}

	packetCount := int(binary.BigEndian.Uint16(payload[:2]))
	cursor := 2

	for i := 0; i < packetCount; i++ {
		if cursor+2 > len(payload) {
			log.Printf("zerodha malformed payload: invalid packet header at index=%d", i)
			return
		}

		packetLen := int(binary.BigEndian.Uint16(payload[cursor : cursor+2]))
		cursor += 2
		if packetLen <= 0 || cursor+packetLen > len(payload) {
			log.Printf("zerodha malformed packet: index=%d packetLen=%d payloadLen=%d", i, packetLen, len(payload))
			return
		}

		packet := payload[cursor : cursor+packetLen]
		cursor += packetLen

		tick, err := s.normalizePacket(packet)
		if err != nil {
			log.Printf("zerodha tick skipped: %v", err)
			continue
		}

		previous, updated, hasPrevious := s.marketState.UpdateWithPrevious(tick)
		if hasPrevious && previous.LTP > 0 {
			changePct := math.Abs(((updated.LTP - previous.LTP) / previous.LTP) * 100)
			if changePct >= spikeAlertPercent {
				log.Printf("abnormal tick spike symbol=%s prev=%.2f curr=%.2f changePercent=%.2f", updated.Symbol, previous.LTP, updated.LTP, changePct)
			}
		}
		if s.tickHub != nil {
			s.tickHub.Publish(updated)
		}
	}
}

func (s *ZerodhaFeedService) normalizePacket(packet []byte) (NormalizedTick, error) {
	if len(packet) < packetMinLength {
		return NormalizedTick{}, fmt.Errorf("packet too short: %d", len(packet))
	}

	token := int64(binary.BigEndian.Uint32(packet[packetOffsetToken : packetOffsetToken+4]))
	ltp := readPrice(packet, packetOffsetLTP)

	meta, ok := s.registry.Get(token)
	if !ok {
		exchange, symbol := sanitizeInstrumentIdentity(token, "", "")
		meta = instrumentMeta{Mode: modeQuote, Exchange: exchange, Symbol: symbol}
	}

	now := time.Now().UTC()
	tick := NormalizedTick{
		Exchange:  meta.Exchange,
		Symbol:    meta.Symbol,
		LTP:       ltp,
		Timestamp: now,
	}

	packetLen := len(packet)

	if packetLen >= packetQuoteOHLCMin && packetLen < packetQuoteOHLCMax {
		tick.Open = readPrice(packet, 8)
		tick.High = readPrice(packet, 12)
		tick.Low = readPrice(packet, 16)
		tick.Close = readPrice(packet, 20)
	}

	if packetLen >= packetFullMin {
		tick.TBQ = int64(readUint32(packet, packetOffsetTBQ))
		tick.TSQ = int64(readUint32(packet, packetOffsetTSQ))
		tick.Open = readPrice(packet, packetOffsetOpen)
		tick.High = readPrice(packet, packetOffsetHigh)
		tick.Low = readPrice(packet, packetOffsetLow)
		tick.Close = readPrice(packet, packetOffsetClose)
	}

	if packetLen >= packetFullWithOI {
		tick.OI = int64(readUint32(packet, packetOffsetOI))
	}

	if packetLen >= packetFullWithDepth && meta.Mode == modeFull {
		if depthStartOffset+depthLevelSize <= packetLen {
			tick.BidQty = int64(readUint32(packet, depthStartOffset))
			tick.BidPrice = readPrice(packet, depthStartOffset+4)
		}
		askStart := depthStartOffset + (depthLevels * depthLevelSize)
		if askStart+depthLevelSize <= packetLen {
			tick.AskQty = int64(readUint32(packet, askStart))
			tick.AskPrice = readPrice(packet, askStart+4)
		}
	}

	if tick.Symbol == "" {
		tick.Symbol = strconv.FormatInt(token, 10)
	}
	if tick.Exchange == "" {
		tick.Exchange = "UNKNOWN"
	}

	return tick, nil
}

func readUint32(packet []byte, offset int) uint32 {
	if offset+4 > len(packet) {
		return 0
	}
	return binary.BigEndian.Uint32(packet[offset : offset+4])
}

func readPrice(packet []byte, offset int) float64 {
	if offset+4 > len(packet) {
		return 0
	}
	return float64(int32(binary.BigEndian.Uint32(packet[offset:offset+4]))) / 100.0
}

func (s *ZerodhaFeedService) seedRegistryFromConfig() {
	indexTokens := parseInstrumentIDs(s.cfg.IndexInstruments)
	for _, token := range indexTokens {
		s.registry.Add(token, instrumentMeta{
			Mode:     modeQuote,
			Exchange: "NSE",
			Symbol:   strconv.FormatInt(token, 10),
		})
	}

	mcxTokens := parseInstrumentIDs(s.cfg.MCXInstruments)
	for _, token := range mcxTokens {
		s.registry.Add(token, instrumentMeta{
			Mode:     modeFull,
			Exchange: "MCX",
			Symbol:   strconv.FormatInt(token, 10),
		})
	}

	if len(indexTokens) > 0 || len(mcxTokens) > 0 {
		return
	}

	mode := TickMode(strings.ToLower(strings.TrimSpace(s.cfg.Mode)))
	if mode != modeQuote && mode != modeFull {
		mode = modeQuote
	}

	fallbackTokens := parseInstrumentIDs(s.cfg.Instruments)
	for _, token := range fallbackTokens {
		s.registry.Add(token, instrumentMeta{
			Mode:     mode,
			Exchange: "UNKNOWN",
			Symbol:   strconv.FormatInt(token, 10),
		})
	}
}

func (s *ZerodhaFeedService) setConnectionState(state ConnectionState) {
	s.stateMu.Lock()
	s.state = state
	s.stateMu.Unlock()
}

func (s *ZerodhaFeedService) setConn(conn *websocket.Conn) {
	s.connMu.Lock()
	if s.conn != nil && s.conn != conn {
		_ = s.conn.Close()
	}
	s.conn = conn
	s.connMu.Unlock()
}

func (s *ZerodhaFeedService) clearConn(conn *websocket.Conn) {
	s.connMu.Lock()
	if s.conn == conn {
		s.conn = nil
	}
	s.connMu.Unlock()
}

func (s *ZerodhaFeedService) getConn() *websocket.Conn {
	s.connMu.RLock()
	conn := s.conn
	s.connMu.RUnlock()
	return conn
}

func sanitizeInstrumentIdentity(token int64, exchange, symbol string) (string, string) {
	if strings.TrimSpace(exchange) == "" {
		exchange = "UNKNOWN"
	}
	if strings.TrimSpace(symbol) == "" {
		symbol = strconv.FormatInt(token, 10)
	}
	return exchange, symbol
}

func parseAuthErrorFromTextMessage(text string) error {
	if isExpiredTokenMessage(text) {
		return fmt.Errorf("%w: %s", errAccessTokenExpired, text)
	}
	if strings.Contains(text, "tokenexception") || strings.Contains(text, "invalid api_key") {
		return fmt.Errorf("%w: %s", errZerodhaAuthFailed, text)
	}
	return nil
}
