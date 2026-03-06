package config

import (
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
)

type ServerConfig struct {
	Host string
	Port string
}

type DatabaseConfig struct {
	Host     string
	User     string
	Password string
	Name     string
	Port     string
	SSLMode  string
}

type ZerodhaConfig struct {
	APIKey           string
	APISecret        string
	RequestToken     string
	AccessToken      string
	WSEndpoint       string
	Instruments      string
	IndexInstruments string
	MCXInstruments   string
	Mode             string
}

type Configuration struct {
	Server   ServerConfig
	Database DatabaseConfig
	Zerodha  ZerodhaConfig
}

var (
	App  Configuration
	once sync.Once
)

func LoadConfig() {
	once.Do(func() {
		if err := godotenv.Load(); err != nil {
			log.Println(".env file not found, using system environment")
		}

		App = Configuration{
			Server: ServerConfig{
				Host: getEnv("SERVER_HOST", "0.0.0.0"),
				Port: getEnv("SERVER_PORT", "8080"),
			},
			Database: DatabaseConfig{
				Host:     getEnv("DB_HOST", "localhost"),
				User:     getEnv("DB_USER", "postgres"),
				Password: getEnv("DB_PASSWORD", ""),
				Name:     getEnv("DB_NAME", "priceFeeder"),
				Port:     getEnv("DB_PORT", "5432"),
				SSLMode:  getEnv("DB_SSLMODE", "disable"),
			},
			Zerodha: ZerodhaConfig{
				APIKey:           getEnv("ZERODHA_API_KEY", ""),
				APISecret:        getEnv("ZERODHA_API_SECRET", ""),
				RequestToken:     getEnv("ZERODHA_REQUEST_TOKEN", ""),
				AccessToken:      getEnv("ZERODHA_ACCESS_TOKEN", ""),
				WSEndpoint:       getEnv("ZERODHA_WS_ENDPOINT", "wss://ws.kite.trade"),
				Instruments:      getEnv("ZERODHA_INSTRUMENTS", ""),
				IndexInstruments: getEnv("ZERODHA_INDEX_INSTRUMENTS", ""),
				MCXInstruments:   getEnv("ZERODHA_MCX_INSTRUMENTS", ""),
				Mode:             getEnv("ZERODHA_MODE", "ltp"),
			},
		}
	})
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
