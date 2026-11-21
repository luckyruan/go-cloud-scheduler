package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/domain"
	"github.com/interview/go-cloud-scheduler/internal/service"
	"github.com/interview/go-cloud-scheduler/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	store        *store.Postgres
	apiKey       string
	claimTimeout time.Duration
	claimed      prometheus.Counter
	completed    *prometheus.CounterVec
	registry     *prometheus.Registry
}

func New(database *store.Postgres, apiKey string, claimTimeout time.Duration) *Server {
	s := &Server{store: database, apiKey: apiKey, claimTimeout: claimTimeout,
		claimed:   prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_executions_claimed_total", Help: "Claimed executions"}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_executions_completed_total", Help: "Completed executions"}, []string{"result"})}
	s.registry = prometheus.NewRegistry()
	s.registry.MustRegister(s.claimed, s.completed)
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "UP"}) })
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("POST /api/v1/tasks", s.auth(s.createTask))
	mux.HandleFunc("GET /api/v1/tasks", s.auth(s.listTasks))
	mux.HandleFunc("POST /api/v1/tasks/{id}/trigger", s.auth(s.trigger))
	mux.HandleFunc("POST /api/v1/workers/heartbeat", s.auth(s.workerHeartbeat))
	mux.HandleFunc("POST /api/v1/executions/claim", s.auth(s.claim))
	mux.HandleFunc("POST /api/v1/executions/{id}/heartbeat", s.auth(s.executionHeartbeat))
	mux.HandleFunc("POST /api/v1/executions/{id}/complete", s.auth(s.complete))
	return requestLog(mux)
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-API-Key")), []byte(s.apiKey)) != 1 {
			writeJSON(w, 401, map[string]string{"error": "invalid API key"})
			return
		}
		next(w, r)
	}
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	task := domain.Task{Enabled: true}
	if !decode(w, r, &task) {
		return
	}
	if err := service.PrepareTask(&task, time.Now()); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if err := s.store.CreateTask(r.Context(), &task); err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, 201, task)
}
func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.ListTasks(r.Context())
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, 200, tasks)
}
func (s *Server) trigger(w http.ResponseWriter, r *http.Request) {
	id, err := s.store.Trigger(r.Context(), r.PathValue("id"), time.Now().UTC())
	if err != nil {
		slog.Warn("trigger rejected", "error", err)
		writeJSON(w, 404, map[string]string{"error": "task unavailable or invalid"})
		return
	}
	writeJSON(w, 202, map[string]string{"executionId": id, "state": "PENDING"})
}
func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var b domain.WorkerHeartbeat
	if !decode(w, r, &b) {
		return
	}
	if strings.TrimSpace(b.WorkerID) == "" {
		writeJSON(w, 400, map[string]string{"error": "workerId is required"})
		return
	}
	if b.Capacity < 1 {
		b.Capacity = 1
	}
	if err := s.store.Heartbeat(r.Context(), b); err != nil {
		serverError(w, err)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) claim(w http.ResponseWriter, r *http.Request) {
	worker := r.URL.Query().Get("workerId")
	if worker == "" {
		writeJSON(w, 400, map[string]string{"error": "workerId is required"})
		return
	}
	e, err := s.store.Claim(r.Context(), worker, time.Now().Add(-s.claimTimeout))
	if err != nil {
		serverError(w, err)
		return
	}
	if e == nil {
		w.WriteHeader(204)
		return
	}
	s.claimed.Inc()
	writeJSON(w, 200, e)
}
func (s *Server) executionHeartbeat(w http.ResponseWriter, r *http.Request) {
	var b struct {
		WorkerID   string `json:"workerId"`
		ClaimToken string `json:"claimToken"`
	}
	if !decode(w, r, &b) {
		return
	}
	if err := s.store.HeartbeatExecution(r.Context(), r.PathValue("id"), b.WorkerID, b.ClaimToken); err != nil {
		writeJSON(w, 409, map[string]string{"error": "execution ownership lost or invalid"})
		return
	}
	w.WriteHeader(204)
}
func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	var c domain.Completion
	if !decode(w, r, &c) {
		return
	}
	if err := s.store.Complete(r.Context(), r.PathValue("id"), c); err != nil {
		slog.Warn("completion rejected", "error", err)
		writeJSON(w, 409, map[string]string{"error": "execution ownership lost or invalid"})
		return
	}
	result := "failed"
	if c.Success {
		result = "succeeded"
	}
	s.completed.WithLabelValues(result).Inc()
	w.WriteHeader(204)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return false
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, 400, map[string]string{"error": "one JSON value is required"})
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func serverError(w http.ResponseWriter, err error) {
	slog.Error("request failed", "error", err)
	writeJSON(w, 500, map[string]string{"error": "internal server error"})
}
func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start))
	})
}

func RunScheduler(ctx context.Context, database *store.Postgres, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			count, err := database.EnqueueDue(ctx, now.UTC(), 100)
			if err != nil {
				slog.Error("schedule scan failed", "error", err)
			} else if count > 0 {
				slog.Info("scheduled executions", "count", count)
			}
		}
	}
}
