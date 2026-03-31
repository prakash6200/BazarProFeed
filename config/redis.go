package config

import (
	"context"
	"log"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

func ConnectRedis() {
	addr := App.Redis.Addr
	if addr == "" {
		addr = "localhost:6379"
	}

	dbIndex, err := strconv.Atoi(App.Redis.DB)
	if err != nil {
		dbIndex = 0
	}

	RedisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: App.Redis.Password,
		DB:       dbIndex,
	})

	ctx := context.Background()
	if _, err := RedisClient.Ping(ctx).Result(); err != nil {
		log.Printf("redis connection warning: %v (feed will operate in-memory only)", err)
		return
	}
	log.Printf("redis connected: %s", addr)
}

func CloseRedis() {
	if RedisClient != nil {
		_ = RedisClient.Close()
	}
}
