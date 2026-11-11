package config

import (
	"os"
)

type APIConfig struct {
	Addr string
}

type WorkerConfig struct {
	// Placeholder for future worker settings
}

func LoadAPIConfig() APIConfig {
	return APIConfig{
		Addr: getenv("API_ADDR", ":8080"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
