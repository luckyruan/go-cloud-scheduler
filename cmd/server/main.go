package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/api"
	"github.com/interview/go-cloud-scheduler/internal/config"
	"github.com/interview/go-cloud-scheduler/internal/store"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database unavailable", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	go api.RunScheduler(ctx, db, cfg.ScheduleInterval)
	server := &http.Server{Addr: cfg.Address, Handler: api.New(db, cfg.APIKey, cfg.ClaimTimeout).Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("scheduler listening", "address", cfg.Address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server failed", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
