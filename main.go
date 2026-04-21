package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"feedprovider/config"
	"feedprovider/controller"
	"feedprovider/models"
	"feedprovider/router"
	"feedprovider/services"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"gorm.io/gorm"
)

func main() {
	config.LoadConfig()
	config.ConnectDatabase()
	config.ConnectRedis()

	config.SeedSecurityData(config.DB)

	// Create Redis caches early so we can clean up before marking expired.
	zerodhaCache := services.NewRedisTickCache(config.RedisClient)
	globalCache := services.NewRedisTickCache(config.RedisClient)

	// 1. Clean Redis keys BEFORE marking — queries need is_deleted=false to find them.
	cleanupExpiredRedisKeys(config.DB, zerodhaCache, globalCache)

	// 2. Now mark expired instruments in DB.
	if count, err := models.MarkExpiredInstruments(config.DB); err != nil {
		log.Printf("failed to mark expired instruments: %v", err)
	} else if count > 0 {
		log.Printf("marked %d expired instruments as deleted/inactive", count)
	}
	if count, err := models.MarkExpiredGlobalInstruments(config.DB); err != nil {
		log.Printf("failed to mark expired global instruments: %v", err)
	} else if count > 0 {
		log.Printf("marked %d expired global instruments as deleted/inactive", count)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tickHub := services.NewTickHub()
	zerodhaFeedService := services.NewZerodhaFeedService(config.App.Zerodha, config.DB, tickHub, zerodhaCache)
	zerodhaFeedService.Start(ctx)

	globMarketTickHub := services.NewTickHub()
	globMarketFeedService := services.NewGlobalMarketFeedService(config.DB, globalCache)
	globMarketFeedService.Start(ctx, globMarketTickHub)
	globSocketHub := config.NewSocketHub(config.DB)

	socketHub := config.NewSocketHub(config.DB)

	defer func() {
		config.CloseRedis()
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	app := fiber.New(fiber.Config{
		BodyLimit:    50 * 1024 * 1024,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	})

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Origin,Content-Type,Accept,Authorization",
	}))

	adminController := controller.NewAdminController(config.DB, socketHub)
	instrumentController := controller.NewAdminInstrumentController(config.DB, zerodhaFeedService)
	globalInstrumentController := controller.NewAdminGlobalInstrumentController(config.DB, globMarketFeedService)
	zerodhaController := controller.NewAdminZerodhaController(config.DB, zerodhaFeedService)
	userController := controller.NewUserController(config.DB, socketHub)

	router.RegisterAdminRoutes(app, adminController, config.DB)
	router.RegisterAdminInstrumentRoutes(app, instrumentController, config.DB)
	router.RegisterAdminGlobalInstrumentRoutes(app, globalInstrumentController, config.DB)
	router.RegisterAdminZerodhaRoutes(app, zerodhaController, config.DB)
	router.RegisterUserRoutes(app, userController, config.DB)

	socketHub.SetInitialStateGetter(func(ctx context.Context, filterSym string) []json.RawMessage {
		ticks := zerodhaCache.Snapshot(ctx, services.ZerodhaTickPrefix, filterSym)
		status := services.MarketStatusOpen
		if !services.IsIndianMarketOpen() {
			status = services.MarketStatusClosed
		}
		msgs := make([]json.RawMessage, 0, len(ticks))
		for _, tick := range ticks {
			tick.MarketStatus = status
			if raw, err := json.Marshal(tick); err == nil {
				msgs = append(msgs, raw)
			}
		}
		return msgs
	})

	globSocketHub.SetInitialStateGetter(func(ctx context.Context, filterSym string) []json.RawMessage {
		ticks := globMarketFeedService.RecentTicks(filterSym)
		if len(ticks) == 0 {
			ticks = globalCache.Snapshot(ctx, services.GlobalTickPrefix, filterSym)
		}
		status := services.MarketStatusOpen
		if !services.IsGlobalMarketOpen() {
			status = services.MarketStatusClosed
		}
		msgs := make([]json.RawMessage, 0, len(ticks))
		for _, tick := range ticks {
			tick.MarketStatus = status
			if raw, err := json.Marshal(tick); err == nil {
				msgs = append(msgs, raw)
			}
		}
		return msgs
	})

	socketHub.RegisterRoutes(app, "/feed")
	socketHub.RegisterSingleInstrumentRoutes(app, "/feed/one")
	socketHub.RegisterBulkInstrumentRoutes(app, "/feed/bulk")
	globSocketHub.RegisterRoutes(app, "/globalfeed")
	globSocketHub.RegisterSingleInstrumentRoutes(app, "/globalfeed/one")
	globSocketHub.RegisterBulkInstrumentRoutes(app, "/globalfeed/bulk")

	events, unsubscribe := tickHub.Subscribe(1024)
	defer unsubscribe()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-events:
				if !ok {
					return
				}
				socketHub.BroadcastJSON(event)
			}
		}
	}()

	globEvents, globUnsubscribe := globMarketTickHub.Subscribe(1024)
	defer globUnsubscribe()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-globEvents:
				if !ok {
					return
				}
				globSocketHub.BroadcastJSON(event)
			}
		}
	}()

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status_code": fiber.StatusOK,
			"message":     "service is healthy",
			"status":      "ok",
			"service":     "feedprovider",
		})
	})

	go func() {
		<-ctx.Done()
		socketHub.CloseAll()
		globSocketHub.CloseAll()
		if err := app.Shutdown(); err != nil {
			log.Printf("failed to shutdown fiber app: %v", err)
		}
	}()

	// Daily cleanup: mark expired instruments and purge Redis keys after market close.
	go dailyExpiryCleanupLoop(ctx, config.DB, zerodhaCache, globalCache, zerodhaFeedService, globMarketFeedService)

	if err := app.Listen(config.App.Server.Host + ":" + config.App.Server.Port); err != nil {
		if ctx.Err() == nil {
			log.Fatal(err)
		}
	}
}

// cleanupExpiredRedisKeys removes Redis tick/circuit keys for instruments that
// have already been identified as expired.
func cleanupExpiredRedisKeys(db *gorm.DB, zerodhaCache, globalCache *services.RedisTickCache) {
	ctx := context.Background()

	// Zerodha instruments
	symbols, tokens, err := models.GetExpiredInstrumentIdentifiers(db)
	if err != nil {
		log.Printf("expiry cleanup: failed to get expired zerodha identifiers: %v", err)
	} else if len(symbols) > 0 {
		del := zerodhaCache.DeleteBySymbols(ctx, services.ZerodhaTickPrefix, symbols)
		del += zerodhaCache.DeleteCircuitByTokens(ctx, tokens)
		log.Printf("expiry cleanup: removed %d zerodha redis keys for %d expired symbols", del, len(symbols))
	}

	// Global instruments
	globSymbols, err := models.GetExpiredGlobalInstrumentSymbols(db)
	if err != nil {
		log.Printf("expiry cleanup: failed to get expired global symbols: %v", err)
	} else if len(globSymbols) > 0 {
		del := globalCache.DeleteBySymbols(ctx, services.GlobalTickPrefix, globSymbols)
		globalCache.RemoveFromSymbolList(ctx, services.GlobalSymbolListKey, globSymbols)
		log.Printf("expiry cleanup: removed %d global redis keys for %d expired symbols", del, len(globSymbols))
	}
}

// dailyExpiryCleanupLoop runs once per day at 16:00 IST (after Indian market close at 15:30)
// to mark expired instruments in DB, purge their Redis cache keys, and unsubscribe
// them from live feed services so they stop receiving data.
func dailyExpiryCleanupLoop(
	ctx context.Context,
	db *gorm.DB,
	zerodhaCache, globalCache *services.RedisTickCache,
	zerodhaFeed *services.ZerodhaFeedService,
	globalFeed *services.GlobalMarketFeedService,
) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		log.Printf("expiry cleanup: failed to load IST timezone: %v, falling back to UTC+5:30", err)
		loc = time.FixedZone("IST", 5*3600+30*60)
	}

	for {
		now := time.Now().In(loc)
		// Next run at 16:00 IST today, or tomorrow if already past.
		next := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, loc)
		if now.After(next) {
			next = next.Add(24 * time.Hour)
		}
		wait := time.Until(next)
		log.Printf("expiry cleanup: next run scheduled at %s (in %s)", next.Format("2006-01-02 15:04 MST"), wait.Round(time.Second))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		// 1. Delete Redis keys BEFORE marking (queries need is_deleted=false).
		cleanupExpiredRedisKeys(db, zerodhaCache, globalCache)

		// 2. Mark expired in DB.
		if count, err := models.MarkExpiredInstruments(db); err != nil {
			log.Printf("expiry cleanup: failed to mark expired instruments: %v", err)
		} else if count > 0 {
			log.Printf("expiry cleanup: marked %d expired instruments as deleted/inactive", count)
		}
		if count, err := models.MarkExpiredGlobalInstruments(db); err != nil {
			log.Printf("expiry cleanup: failed to mark expired global instruments: %v", err)
		} else if count > 0 {
			log.Printf("expiry cleanup: marked %d expired global instruments as deleted/inactive", count)
		}

		// 3. Refresh feed services so they unsubscribe expired symbols from
		//    their in-memory lists and live WebSocket connections.
		if err := zerodhaFeed.RefreshFromDatabase(ctx); err != nil {
			log.Printf("expiry cleanup: zerodha feed refresh failed: %v", err)
		}
		if err := globalFeed.RefreshFromDatabase(ctx); err != nil {
			log.Printf("expiry cleanup: global feed refresh failed: %v", err)
		}
	}
}
