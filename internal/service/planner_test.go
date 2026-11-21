package service

import (
	"testing"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/domain"
)

func TestPrepareTask(t *testing.T) {
	now := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	task := domain.Task{Name: " report ", CronExpression: "*/5 * * * *", MaxRetries: 3}
	if err := PrepareTask(&task, now); err != nil {
		t.Fatal(err)
	}
	if task.Name != "report" || task.TimeoutSeconds != 60 {
		t.Fatalf("defaults not applied: %+v", task)
	}
	want := now.Add(5 * time.Minute)
	if !task.NextRunAt.Equal(want) {
		t.Fatalf("next run=%v want=%v", task.NextRunAt, want)
	}
}

func TestRejectsInvalidCronAndRetries(t *testing.T) {
	if err := PrepareTask(&domain.Task{Name: "x", CronExpression: "bad"}, time.Now()); err == nil {
		t.Fatal("expected cron error")
	}
	if err := PrepareTask(&domain.Task{Name: "x", CronExpression: "@hourly", MaxRetries: 11}, time.Now()); err == nil {
		t.Fatal("expected retry error")
	}
}

func TestRetryDelayIsBoundedExponential(t *testing.T) {
	if RetryDelay(1) != time.Second || RetryDelay(4) != 8*time.Second || RetryDelay(20) != 128*time.Second {
		t.Fatal("unexpected retry delays")
	}
}

func TestPreparePreservesDisabledAndBoundsTimeout(t *testing.T) {
	task := domain.Task{Name: "disabled", CronExpression: "@hourly", Enabled: false}
	if err := PrepareTask(&task, time.Now()); err != nil || task.Enabled {
		t.Fatalf("disabled task changed: %+v %v", task, err)
	}
	task.TimeoutSeconds = 3601
	if err := PrepareTask(&task, time.Now()); err == nil {
		t.Fatal("accepted unbounded timeout")
	}
}
