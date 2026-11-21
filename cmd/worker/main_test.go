package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/domain"
)

func TestLongJobRenewsAndCompletionCarriesOwnership(t *testing.T) {
	var renewals atomic.Int32
	completed := make(chan domain.Completion, 1)
	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/complete") {
			var c domain.Completion
			if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
				t.Error(err)
			}
			completed <- c
		} else if strings.Contains(r.URL.Path, "/executions/") {
			var b map[string]string
			if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
				t.Error(err)
			}
			if b["claimToken"] != "owner-token" {
				t.Error("renewal lost token")
			}
			renewals.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer scheduler.Close()
	job := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") != "e1" || r.Header.Get("X-Scheduler-Execution-ID") != "e1" {
			t.Error("missing stable downstream deduplication key")
		}
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for renewals.Load() == 0 {
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Error("no execution renewal")
				return
			}
		}
		_, _ = w.Write([]byte("done"))
	}))
	defer job.Close()
	worker := worker{baseURL: scheduler.URL, id: "w1", client: scheduler.Client(), jobClient: job.Client(), heartbeatInterval: 5 * time.Millisecond}
	worker.execute(context.Background(), &domain.Execution{ID: "e1", ClaimToken: "owner-token", TimeoutSecs: 3, Payload: map[string]any{"url": job.URL}})
	select {
	case c := <-completed:
		if !c.Success || c.ClaimToken != "owner-token" || c.Output != "done" {
			t.Fatalf("completion=%+v", c)
		}
	default:
		t.Fatal("missing completion")
	}
}
