package domain

import "time"

type Task struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	CronExpression string         `json:"cronExpression"`
	Payload        map[string]any `json:"payload"`
	TimeoutSeconds int            `json:"timeoutSeconds"`
	MaxRetries     int            `json:"maxRetries"`
	Enabled        bool           `json:"enabled"`
	NextRunAt      time.Time      `json:"nextRunAt"`
	CreatedAt      time.Time      `json:"createdAt"`
}

type Execution struct {
	ID          string         `json:"id"`
	TaskID      string         `json:"taskId"`
	TaskName    string         `json:"taskName"`
	Payload     map[string]any `json:"payload"`
	Attempt     int            `json:"attempt"`
	MaxRetries  int            `json:"maxRetries"`
	TimeoutSecs int            `json:"timeoutSeconds"`
	Status      string         `json:"status"`
	WorkerID    string         `json:"workerId,omitempty"`
	ClaimToken  string         `json:"claimToken,omitempty"`
	ScheduledAt time.Time      `json:"scheduledAt"`
	StartedAt   *time.Time     `json:"startedAt,omitempty"`
}

type WorkerHeartbeat struct {
	WorkerID string            `json:"workerId"`
	Labels   map[string]string `json:"labels"`
	Capacity int               `json:"capacity"`
}

type Completion struct {
	WorkerID   string `json:"workerId"`
	ClaimToken string `json:"claimToken"`
	Success    bool   `json:"success"`
	Output     string `json:"output"`
	Error      string `json:"error"`
}
