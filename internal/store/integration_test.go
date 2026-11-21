package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/domain"
	"github.com/interview/go-cloud-scheduler/internal/service"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRetryBudgetAndOldClaimRejection(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; PostgreSQL test skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "scheduler_test_" + hex.EncodeToString(random[:])
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	}()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// Skip CREATE EXTENSION: gen_random_uuid is built-in on supported PostgreSQL 17.
	sql, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	start := 0
	for start < len(sql) && sql[start] != '\n' {
		start++
	}
	if _, err := pool.Exec(ctx, string(sql[start:])); err != nil {
		t.Fatal(err)
	}
	tokenSQL, err := os.ReadFile("../../migrations/002_claim_token.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(tokenSQL)); err != nil {
		t.Fatal(err)
	}
	store := &Postgres{Pool: pool}
	task := domain.Task{Name: "bounded", CronExpression: "@hourly", MaxRetries: 1, Enabled: false}
	if err := service.PrepareTask(&task, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTask(ctx, &task); err != nil {
		t.Fatal(err)
	}
	id, err := store.Trigger(ctx, task.ID, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Claim(ctx, "same-worker", time.Now().Add(-30*time.Second))
	if err != nil || first == nil {
		t.Fatalf("claim=%+v err=%v", first, err)
	}
	if err := store.Complete(ctx, id, domain.Completion{WorkerID: "same-worker", ClaimToken: first.ClaimToken, Success: false}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE executions SET scheduled_at=now()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	second, err := store.Claim(ctx, "same-worker", time.Now().Add(-30*time.Second))
	if err != nil || second == nil {
		t.Fatalf("claim=%+v err=%v", second, err)
	}
	if second.ID != id || second.Attempt != 2 || second.ClaimToken == first.ClaimToken {
		t.Fatalf("retry state=%+v", second)
	}
	if err := store.Complete(ctx, id, domain.Completion{WorkerID: "same-worker", ClaimToken: first.ClaimToken, Success: true}); err == nil {
		t.Fatal("old claimant changed result")
	}
	if err := store.Complete(ctx, id, domain.Completion{WorkerID: "same-worker", ClaimToken: second.ClaimToken, Success: false}); err != nil {
		t.Fatal(err)
	}
	value, err := store.Claim(ctx, "same-worker", time.Now().Add(-30*time.Second))
	if err != nil || value != nil {
		t.Fatalf("retry budget not exhausted: %+v %v", value, err)
	}
	// An expired final lease must be finalized even if there is nothing else to claim.
	if _, err := pool.Exec(ctx, "UPDATE executions SET status='RUNNING',heartbeat_at=now()-interval '1 minute' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	value, err = store.Claim(ctx, "same-worker", time.Now().Add(-30*time.Second))
	if err != nil || value != nil {
		t.Fatalf("expired final claim: %+v %v", value, err)
	}
	var finalStatus string
	if err := pool.QueryRow(ctx, "SELECT status FROM executions WHERE id=$1", id).Scan(&finalStatus); err != nil || finalStatus != "FAILED" {
		t.Fatalf("final lease status=%s err=%v", finalStatus, err)
	}
}
