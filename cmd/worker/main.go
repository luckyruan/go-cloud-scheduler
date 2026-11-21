package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/domain"
)

type worker struct {
	baseURL           string
	apiKey            string
	id                string
	client            *http.Client
	jobClient         *http.Client
	heartbeatInterval time.Duration
}

type httpJob struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    any               `json:"body"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	w := &worker{
		baseURL:   strings.TrimRight(env("SCHEDULER_URL", "http://localhost:8090"), "/"),
		apiKey:    env("API_KEY", "local-scheduler-key"),
		id:        env("WORKER_ID", hostname()),
		client:    &http.Client{Timeout: 35 * time.Second},
		jobClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	slog.Info("worker started", "workerId", w.id, "scheduler", w.baseURL)
	w.run(ctx)
}

func (w *worker) run(ctx context.Context) {
	heartbeat := time.NewTicker(10 * time.Second)
	poll := time.NewTicker(time.Second)
	defer heartbeat.Stop()
	defer poll.Stop()
	_ = w.heartbeat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if err := w.heartbeat(ctx); err != nil {
				slog.Warn("worker heartbeat failed", "error", err)
			}
		case <-poll.C:
			execution, err := w.claim(ctx)
			if err != nil {
				slog.Warn("claim failed", "error", err)
				continue
			}
			if execution != nil {
				w.execute(ctx, execution)
			}
		}
	}
}

func (w *worker) heartbeat(ctx context.Context) error {
	body := domain.WorkerHeartbeat{WorkerID: w.id, Capacity: 1, Labels: map[string]string{"executor": "http"}}
	err := w.jsonRequest(ctx, http.MethodPost, "/api/v1/workers/heartbeat", body, nil)
	if errors.Is(err, errNoContent) {
		return nil
	}
	return err
}

func (w *worker) claim(ctx context.Context) (*domain.Execution, error) {
	var execution domain.Execution
	err := w.jsonRequest(ctx, http.MethodPost, "/api/v1/executions/claim?workerId="+url.QueryEscape(w.id), nil, &execution)
	if errors.Is(err, errNoContent) {
		return nil, nil
	}
	return &execution, err
}

func (w *worker) execute(parent context.Context, execution *domain.Execution) {
	started := time.Now()
	jobBytes, _ := json.Marshal(execution.Payload)
	var job httpJob
	if err := json.Unmarshal(jobBytes, &job); err != nil || job.URL == "" {
		w.complete(parent, execution, false, "", "payload must contain a valid url")
		return
	}
	if job.Method == "" {
		job.Method = http.MethodGet
	}
	timeout := time.Duration(execution.TimeoutSecs) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	beatCtx, stopBeat := context.WithCancel(ctx)
	beatDone := make(chan struct{})
	go func() { defer close(beatDone); w.renew(beatCtx, execution, cancel) }()
	defer func() { stopBeat(); <-beatDone }()

	var body io.Reader
	if job.Body != nil {
		encoded, _ := json.Marshal(job.Body)
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(job.Method), job.URL, body)
	if err != nil {
		w.complete(parent, execution, false, "", err.Error())
		return
	}
	for key, value := range job.Headers {
		req.Header.Set(key, value)
	}
	// Retries keep the execution ID; downstream handlers must implement deduplication.
	req.Header.Set("Idempotency-Key", execution.ID)
	req.Header.Set("X-Scheduler-Execution-ID", execution.ID)
	if job.Body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	client := w.jobClient
	if client == nil {
		client = w.client
	}
	resp, err := client.Do(req)
	if err != nil {
		w.complete(parent, execution, false, "", err.Error())
		return
	}
	defer resp.Body.Close()
	result, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	success := resp.StatusCode >= 200 && resp.StatusCode < 300 && readErr == nil
	errText := ""
	if readErr != nil {
		errText = "reading upstream response failed: " + readErr.Error()
	} else if !success {
		errText = fmt.Sprintf("upstream returned %s", resp.Status)
	}
	w.complete(parent, execution, success, string(result), errText)
	slog.Info("execution finished", "executionId", execution.ID, "success", success, "duration", time.Since(started))
}

func (w *worker) complete(parent context.Context, execution *domain.Execution, success bool, output, errorText string) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	body := domain.Completion{WorkerID: w.id, ClaimToken: execution.ClaimToken, Success: success, Output: output, Error: errorText}
	if err := w.jsonRequest(ctx, http.MethodPost, "/api/v1/executions/"+execution.ID+"/complete", body, nil); err != nil && !errors.Is(err, errNoContent) {
		slog.Error("completion report failed", "executionId", execution.ID, "error", err)
	}
}

func (w *worker) renew(ctx context.Context, execution *domain.Execution, cancelJob context.CancelFunc) {
	interval := w.heartbeatInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := w.jsonRequest(callCtx, http.MethodPost, "/api/v1/executions/"+execution.ID+"/heartbeat",
				map[string]string{"workerId": w.id, "claimToken": execution.ClaimToken}, nil)
			if err == nil || errors.Is(err, errNoContent) {
				_ = w.heartbeat(callCtx)
			}
			cancel()
			if err != nil && !errors.Is(err, errNoContent) && ctx.Err() == nil {
				slog.Warn("execution renewal failed; canceling local HTTP work", "executionId", execution.ID, "error", err)
				cancelJob()
				return
			}
		}
	}
}

var errNoContent = errors.New("no content")

func (w *worker) jsonRequest(ctx context.Context, method, path string, body, output any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, w.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", w.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return errNoContent
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("scheduler returned %s: %s", resp.Status, data)
	}
	if output != nil {
		return json.NewDecoder(resp.Body).Decode(output)
	}
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "worker-local"
	}
	return name
}
