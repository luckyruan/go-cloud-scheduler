package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Address          string
	DatabaseURL      string
	APIKey           string
	ScheduleInterval time.Duration
	ClaimTimeout     time.Duration
}

func Load() Config {
	c := Config{
		Address:          env("ADDRESS", ":8090"),
		DatabaseURL:      env("DATABASE_URL", "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"),
		APIKey:           env("API_KEY", "local-scheduler-key"),
		ScheduleInterval: duration("SCHEDULE_INTERVAL", time.Second),
		ClaimTimeout:     duration("CLAIM_TIMEOUT", 30*time.Second),
	}
	if c.ClaimTimeout < 10*time.Second {
		c.ClaimTimeout = 30 * time.Second
	}
	return c
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func duration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 && parsed <= time.Hour {
		return parsed
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 && seconds <= 3600 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}
