package services

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"feedprovider/config"

	"github.com/gorilla/websocket"
)

var (
	errZerodhaAuthFailed  = errors.New("zerodha authentication failed")
	errAccessTokenExpired = errors.New("zerodha access token expired")
)

type kiteErrorResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"error_type"`
	Message   string `json:"message"`
}

type ZerodhaFeedService struct {
	cfg     config.ZerodhaConfig
	tickHub *TickHub
}

type subscriptionPlan struct {
	allTokens       []int64
	indexTokens     []int64
	mcxTokens       []int64
	useSegmentModes bool
}

func NewZerodhaFeedService(cfg config.ZerodhaConfig, tickHub *TickHub) *ZerodhaFeedService {
	return &ZerodhaFeedService{cfg: cfg, tickHub: tickHub}
}

func (s *ZerodhaFeedService) Start(ctx context.Context) {
	if strings.TrimSpace(s.cfg.APIKey) == "" || strings.TrimSpace(s.cfg.AccessToken) == "" {
		log.Println("zerodha feed skipped: set ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN in .env")
		return
	}

	plan := s.buildSubscriptionPlan()
	if len(plan.allTokens) == 0 {
		log.Println("zerodha feed skipped: set ZERODHA_INDEX_INSTRUMENTS and/or ZERODHA_MCX_INSTRUMENTS (fallback: ZERODHA_INSTRUMENTS)")
		return
	}

	if err := s.validateAccessToken(ctx); err != nil {
		s.handleAuthFailure(err)
		return
	}

	go s.runReconnectLoop(ctx, plan)
}

func (s *ZerodhaFeedService) runReconnectLoop(ctx context.Context, plan subscriptionPlan) {
	backoff := 2 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			return
		}

		err := s.connectAndRead(ctx, plan)
		if err != nil && ctx.Err() == nil {
			if isAuthFailure(err) {
				s.handleAuthFailure(err)
				return
			}
			log.Printf("zerodha feed error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func (s *ZerodhaFeedService) connectAndRead(ctx context.Context, plan subscriptionPlan) error {
	wsURL, err := url.Parse(s.cfg.WSEndpoint)
	if err != nil {
		return err
	}

	query := wsURL.Query()
	query.Set("api_key", s.cfg.APIKey)
	query.Set("access_token", s.cfg.AccessToken)
	wsURL.RawQuery = query.Encode()

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.Dial(wsURL.String(), nil)
	if err != nil {
		return wrapDialError(err, resp)
	}
	defer conn.Close()

	log.Println("zerodha websocket connected")

	if err := writeJSON(conn, map[string]any{"a": "subscribe", "v": plan.allTokens}); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	if err := s.applyModes(conn, plan); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.BinaryMessage {
			continue
		}

		s.parseAndPublishTicks(payload)
	}
}

func (s *ZerodhaFeedService) buildSubscriptionPlan() subscriptionPlan {
	indexTokens := uniqueTokens(parseInstrumentIDs(s.cfg.IndexInstruments))
	mcxTokens := uniqueTokens(parseInstrumentIDs(s.cfg.MCXInstruments))

	if len(indexTokens) > 0 || len(mcxTokens) > 0 {
		allTokens := uniqueTokens(append(append([]int64{}, indexTokens...), mcxTokens...))
		return subscriptionPlan{
			allTokens:       allTokens,
			indexTokens:     indexTokens,
			mcxTokens:       mcxTokens,
			useSegmentModes: true,
		}
	}

	allTokens := uniqueTokens(parseInstrumentIDs(s.cfg.Instruments))
	return subscriptionPlan{allTokens: allTokens, useSegmentModes: false}
}

func (s *ZerodhaFeedService) applyModes(conn *websocket.Conn, plan subscriptionPlan) error {
	if plan.useSegmentModes {
		if len(plan.indexTokens) > 0 {
			if err := writeJSON(conn, map[string]any{"a": "mode", "v": []any{"quote", plan.indexTokens}}); err != nil {
				return fmt.Errorf("set index mode failed: %w", err)
			}
		}
		if len(plan.mcxTokens) > 0 {
			if err := writeJSON(conn, map[string]any{"a": "mode", "v": []any{"full", plan.mcxTokens}}); err != nil {
				return fmt.Errorf("set mcx mode failed: %w", err)
			}
		}
		return nil
	}

	if err := writeJSON(conn, map[string]any{"a": "mode", "v": []any{s.cfg.Mode, plan.allTokens}}); err != nil {
		return fmt.Errorf("set mode failed: %w", err)
	}

	return nil
}

func (s *ZerodhaFeedService) validateAccessToken(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.kite.trade/user/profile", nil)
	if err != nil {
		return fmt.Errorf("token validation request build failed: %w", err)
	}

	req.Header.Set("X-Kite-Version", "3")
	req.Header.Set("Authorization", fmt.Sprintf("token %s:%s", s.cfg.APIKey, s.cfg.AccessToken))

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("token validation request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

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

	return fmt.Errorf("token validation failed with status=%d", resp.StatusCode)
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

func writeJSON(conn *websocket.Conn, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

func parseInstrumentIDs(raw string) []int64 {
	parts := strings.Split(raw, ",")
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
		result = append(result, id)
	}
	return result
}

func uniqueTokens(tokens []int64) []int64 {
	seen := make(map[int64]struct{}, len(tokens))
	unique := make([]int64, 0, len(tokens))
	for _, token := range tokens {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		unique = append(unique, token)
	}
	return unique
}

func (s *ZerodhaFeedService) parseAndPublishTicks(payload []byte) {
	if len(payload) < 2 {
		return
	}

	packetCount := int(binary.BigEndian.Uint16(payload[:2]))
	cursor := 2

	for i := 0; i < packetCount; i++ {
		if cursor+2 > len(payload) {
			return
		}
		packetLen := int(binary.BigEndian.Uint16(payload[cursor : cursor+2]))
		cursor += 2

		if cursor+packetLen > len(payload) {
			return
		}
		packet := payload[cursor : cursor+packetLen]
		cursor += packetLen

		if len(packet) < 8 {
			continue
		}

		instrumentToken := int64(binary.BigEndian.Uint32(packet[0:4]))
		lastPrice := float64(int32(binary.BigEndian.Uint32(packet[4:8]))) / 100.0
		if s.tickHub != nil {
			s.tickHub.Publish(TickEvent{
				InstrumentToken: instrumentToken,
				LTP:             lastPrice,
				Timestamp:       time.Now().UTC(),
			})
		}
		log.Printf("zerodha tick instrument=%d ltp=%.2f", instrumentToken, lastPrice)
	}
}
