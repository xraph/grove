package extension

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/xraph/grove/kv"
)

// Queue provides a simple distributed job queue backed by a KV store.
type Queue struct {
	store             *kv.Store
	prefix            string
	visibilityTimeout time.Duration
}

// Job represents a job in the queue.
type Job struct {
	ID         string    `json:"id"`
	Payload    any       `json:"payload"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// NewQueue creates a new job queue.
func NewQueue(store *kv.Store, prefix string, opts ...QueueOption) *Queue {
	q := &Queue{
		store:             store,
		prefix:            "queue:" + prefix,
		visibilityTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// QueueOption configures a Queue.
type QueueOption func(*Queue)

// WithVisibilityTimeout sets the visibility timeout for dequeued jobs.
func WithVisibilityTimeout(d time.Duration) QueueOption {
	return func(q *Queue) { q.visibilityTimeout = d }
}

// Enqueue adds a job to the queue.
func (q *Queue) Enqueue(ctx context.Context, payload any) (string, error) {
	id := generateJobID()
	job := Job{
		ID:         id,
		Payload:    payload,
		EnqueuedAt: time.Now(),
	}
	key := q.jobKey(id)
	if err := q.store.Set(ctx, key, job); err != nil {
		return "", fmt.Errorf("queue: enqueue: %w", err)
	}

	// Add to the job index.
	var index []string
	_ = q.store.Get(ctx, q.indexKey(), &index)
	index = append(index, id)
	if err := q.store.Set(ctx, q.indexKey(), index); err != nil {
		return "", fmt.Errorf("queue: enqueue index: %w", err)
	}

	return id, nil
}

// Dequeue retrieves and locks the next job from the queue.
func (q *Queue) Dequeue(ctx context.Context) (*Job, error) {
	var index []string
	if err := q.store.Get(ctx, q.indexKey(), &index); err != nil {
		if err == kv.ErrNotFound {
			return nil, nil // empty queue
		}
		return nil, fmt.Errorf("queue: dequeue index: %w", err)
	}

	if len(index) == 0 {
		return nil, nil
	}

	// Take the first job.
	jobID := index[0]
	remaining := index[1:]

	// Update index.
	if err := q.store.Set(ctx, q.indexKey(), remaining); err != nil {
		return nil, fmt.Errorf("queue: dequeue update index: %w", err)
	}

	// Get job data.
	var job Job
	if err := q.store.Get(ctx, q.jobKey(jobID), &job); err != nil {
		return nil, fmt.Errorf("queue: dequeue get job: %w", err)
	}

	// Move to processing set with visibility timeout.
	if err := q.store.Set(ctx, q.processingKey(jobID), job, kv.WithTTL(q.visibilityTimeout)); err != nil {
		return nil, fmt.Errorf("queue: dequeue processing: %w", err)
	}

	return &job, nil
}

// Ack acknowledges that a job has been processed and removes it.
func (q *Queue) Ack(ctx context.Context, jobID string) error {
	if err := q.store.Delete(ctx, q.jobKey(jobID), q.processingKey(jobID)); err != nil {
		return fmt.Errorf("queue: ack: %w", err)
	}
	return nil
}

// Size returns the number of pending jobs in the queue.
func (q *Queue) Size(ctx context.Context) (int, error) {
	var index []string
	if err := q.store.Get(ctx, q.indexKey(), &index); err != nil {
		if err == kv.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	return len(index), nil
}

func (q *Queue) jobKey(id string) string {
	return q.prefix + ":job:" + id
}

func (q *Queue) indexKey() string {
	return q.prefix + ":index"
}

func (q *Queue) processingKey(id string) string {
	return q.prefix + ":processing:" + id
}

func generateJobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
