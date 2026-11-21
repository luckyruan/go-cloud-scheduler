package service

import (
	"errors"
	"strings"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/domain"
	"github.com/robfig/cron/v3"
)

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func PrepareTask(task *domain.Task, now time.Time) error {
	task.Name = strings.TrimSpace(task.Name)
	if task.Name == "" || len(task.Name) > 160 {
		return errors.New("name must contain 1-160 bytes")
	}
	if task.CronExpression == "" {
		return errors.New("cronExpression is required")
	}
	schedule, err := parser.Parse(task.CronExpression)
	if err != nil {
		return errors.New("invalid cronExpression: " + err.Error())
	}
	if task.TimeoutSeconds <= 0 {
		task.TimeoutSeconds = 60
	}
	if task.TimeoutSeconds > 3600 {
		return errors.New("timeoutSeconds must not exceed 3600")
	}
	if task.MaxRetries < 0 || task.MaxRetries > 10 {
		return errors.New("maxRetries must be between 0 and 10")
	}
	if task.Payload == nil {
		task.Payload = map[string]any{}
	}
	task.NextRunAt = schedule.Next(now.UTC())
	return nil
}

func NextRun(expression string, after time.Time) (time.Time, error) {
	schedule, err := parser.Parse(expression)
	if err != nil {
		return time.Time{}, err
	}
	return schedule.Next(after.UTC()), nil
}

func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<(attempt-1)) * time.Second
}
