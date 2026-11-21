package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/interview/go-cloud-scheduler/internal/domain"
	"github.com/interview/go-cloud-scheduler/internal/service"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{Pool: pool}, nil
}

func (p *Postgres) Close() { p.Pool.Close() }

func (p *Postgres) CreateTask(ctx context.Context, task *domain.Task) error {
	payload, _ := json.Marshal(task.Payload)
	return p.Pool.QueryRow(ctx, `INSERT INTO tasks(name,cron_expression,payload,timeout_seconds,max_retries,enabled,next_run_at)
		VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,created_at`, task.Name, task.CronExpression, payload,
		task.TimeoutSeconds, task.MaxRetries, task.Enabled, task.NextRunAt).Scan(&task.ID, &task.CreatedAt)
}

func (p *Postgres) ListTasks(ctx context.Context) ([]domain.Task, error) {
	rows, err := p.Pool.Query(ctx, `SELECT id,name,cron_expression,payload,timeout_seconds,max_retries,enabled,next_run_at,created_at FROM tasks ORDER BY created_at DESC LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Task
	for rows.Next() {
		var task domain.Task
		var payload []byte
		if err := rows.Scan(&task.ID, &task.Name, &task.CronExpression, &payload, &task.TimeoutSeconds, &task.MaxRetries, &task.Enabled, &task.NextRunAt, &task.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(payload, &task.Payload)
		result = append(result, task)
	}
	return result, rows.Err()
}

func (p *Postgres) Trigger(ctx context.Context, taskID string, when time.Time) (string, error) {
	var id string
	err := p.Pool.QueryRow(ctx, `INSERT INTO executions(task_id,scheduled_at) SELECT id,$2 FROM tasks WHERE id=$1 RETURNING id`, taskID, when).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", errors.New("task not found")
	}
	return id, err
}

func (p *Postgres) EnqueueDue(ctx context.Context, now time.Time, limit int) (int, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `SELECT id,cron_expression,next_run_at FROM tasks WHERE enabled=true AND next_run_at<=$1 ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT $2`, now, limit)
	if err != nil {
		return 0, err
	}
	type due struct {
		id, expression string
		at             time.Time
	}
	var tasks []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.id, &d.expression, &d.at); err != nil {
			rows.Close()
			return 0, err
		}
		tasks = append(tasks, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, task := range tasks {
		if _, err = tx.Exec(ctx, `INSERT INTO executions(task_id,scheduled_at) VALUES($1,$2)`, task.id, task.at); err != nil {
			return 0, err
		}
		next, parseErr := service.NextRun(task.expression, task.at)
		if parseErr != nil {
			return 0, parseErr
		}
		if _, err = tx.Exec(ctx, `UPDATE tasks SET next_run_at=$2,updated_at=now() WHERE id=$1`, task.id, next); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(tasks), nil
}

func (p *Postgres) Heartbeat(ctx context.Context, beat domain.WorkerHeartbeat) error {
	labels, _ := json.Marshal(beat.Labels)
	_, err := p.Pool.Exec(ctx, `INSERT INTO workers(id,labels,capacity,last_seen_at) VALUES($1,$2,$3,now())
		ON CONFLICT(id) DO UPDATE SET labels=excluded.labels,capacity=excluded.capacity,last_seen_at=now()`, beat.WorkerID, labels, beat.Capacity)
	return err
}

func (p *Postgres) Claim(ctx context.Context, workerID string, staleBefore time.Time) (*domain.Execution, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, `UPDATE executions e SET status='FAILED',finished_at=now(),
		claim_token=NULL,error_message='lease expired after final attempt'
		FROM tasks t WHERE t.id=e.task_id AND e.status='RUNNING' AND e.heartbeat_at<$1
		AND e.attempt>=t.max_retries+1`, staleBefore); err != nil {
		return nil, err
	}
	var e domain.Execution
	var payload []byte
	err = tx.QueryRow(ctx, `WITH candidate AS (
		SELECT e.id FROM executions e JOIN tasks t ON t.id=e.task_id
		WHERE e.attempt<t.max_retries+1 AND ((e.status='PENDING' AND e.scheduled_at<=now()) OR (e.status='RUNNING' AND e.heartbeat_at<$2))
		ORDER BY e.scheduled_at,e.id FOR UPDATE OF e SKIP LOCKED LIMIT 1)
		UPDATE executions e SET status='RUNNING',worker_id=$1,started_at=now(),heartbeat_at=now(),attempt=attempt+1,claim_token=gen_random_uuid(),finished_at=NULL
		FROM candidate c,tasks t WHERE e.id=c.id AND t.id=e.task_id
		RETURNING e.id,e.task_id,t.name,t.payload,e.attempt,t.max_retries,t.timeout_seconds,e.status,e.worker_id,e.claim_token::text,e.scheduled_at,e.started_at`, workerID, staleBefore).
		Scan(&e.ID, &e.TaskID, &e.TaskName, &payload, &e.Attempt, &e.MaxRetries, &e.TimeoutSecs, &e.Status, &e.WorkerID, &e.ClaimToken, &e.ScheduledAt, &e.StartedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Persist exhausted-lease cleanup even when no new candidate is available.
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(payload, &e.Payload)
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &e, nil
}

func (p *Postgres) Complete(ctx context.Context, executionID string, completion domain.Completion) error {
	if completion.ClaimToken == "" {
		return errors.New("claimToken is required")
	}
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var taskID, status string
	var attempt, maxRetries int
	err = tx.QueryRow(ctx, `SELECT e.task_id,e.status,e.attempt,t.max_retries FROM executions e JOIN tasks t ON t.id=e.task_id WHERE e.id=$1 AND e.worker_id=$2 AND e.claim_token=$3::uuid FOR UPDATE OF e`, executionID, completion.WorkerID, completion.ClaimToken).Scan(&taskID, &status, &attempt, &maxRetries)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("execution not found or worker mismatch")
	}
	if err != nil {
		return err
	}
	if status != "RUNNING" {
		return errors.New("execution is not running")
	}
	finalStatus := "SUCCEEDED"
	if !completion.Success {
		finalStatus = "FAILED"
	}
	if !completion.Success && attempt <= maxRetries {
		delay := service.RetryDelay(attempt)
		// Keep attempt on the same row; a fresh row would reset the retry budget.
		_, err = tx.Exec(ctx, `UPDATE executions SET status='PENDING',scheduled_at=now()+($2::double precision*interval '1 second'),
			claim_token=NULL,worker_id=NULL,heartbeat_at=NULL,output=$3,error_message=$4,finished_at=NULL
			WHERE id=$1`, executionID, delay.Seconds(), completion.Output, completion.Error)
	} else {
		_, err = tx.Exec(ctx, `UPDATE executions SET status=$2,output=$3,error_message=$4,finished_at=now(),heartbeat_at=now(),claim_token=NULL WHERE id=$1`, executionID, finalStatus, completion.Output, completion.Error)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) HeartbeatExecution(ctx context.Context, executionID, workerID, token string) error {
	if token == "" {
		return errors.New("claimToken is required")
	}
	tag, err := p.Pool.Exec(ctx, `UPDATE executions SET heartbeat_at=now() WHERE id=$1 AND worker_id=$2 AND claim_token=$3::uuid AND status='RUNNING'`, executionID, workerID, token)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("running execution not found")
	}
	return err
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
