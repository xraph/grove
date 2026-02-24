package extension_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/extension"
	"github.com/xraph/grove/kv/kvtest"
)

func TestQueue_Enqueue(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs")

	id, err := q.Enqueue(context.Background(), map[string]string{"task": "send_email"})
	require.NoError(t, err)
	assert.NotEmpty(t, id)
}

func TestQueue_Dequeue_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs")

	ctx := context.Background()
	payload := map[string]string{"task": "send_email"}
	_, err := q.Enqueue(ctx, payload)
	require.NoError(t, err)

	job, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.NotEmpty(t, job.ID)

	// The payload round-trips through JSON, so assert against the decoded form.
	jobPayload, ok := job.Payload.(map[string]any)
	require.True(t, ok, "expected payload to be map[string]any, got %T", job.Payload)
	assert.Equal(t, "send_email", jobPayload["task"])
}

func TestQueue_Dequeue_FIFO(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs")

	ctx := context.Background()
	idA, err := q.Enqueue(ctx, "A")
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, "B")
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, "C")
	require.NoError(t, err)

	job, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, idA, job.ID, "first dequeue should return job A")
}

func TestQueue_Dequeue_EmptyQueue(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs")

	job, err := q.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, job)
}

func TestQueue_Ack(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs")

	ctx := context.Background()
	_, err := q.Enqueue(ctx, "do-work")
	require.NoError(t, err)

	job, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, job)

	err = q.Ack(ctx, job.ID)
	require.NoError(t, err)
}

func TestQueue_Size(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs")

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := q.Enqueue(ctx, i)
		require.NoError(t, err)
	}

	size, err := q.Size(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, size)
}

func TestQueue_Size_Empty(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs")

	size, err := q.Size(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, size)
}

func TestQueue_VisibilityTimeout(t *testing.T) {
	store := kvtest.SetupStore(t)
	timeout := 45 * time.Second
	q := extension.NewQueue(store, "jobs", extension.WithVisibilityTimeout(timeout))

	// Verify the queue was constructed without error; the option is accepted.
	ctx := context.Background()
	_, err := q.Enqueue(ctx, "payload")
	require.NoError(t, err)

	job, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, job)
}

func TestQueue_CustomVisibilityTimeout(t *testing.T) {
	store := kvtest.SetupStore(t)
	q := extension.NewQueue(store, "jobs", extension.WithVisibilityTimeout(10*time.Second))

	// Enqueue and dequeue to verify the custom timeout does not break behavior.
	ctx := context.Background()
	_, err := q.Enqueue(ctx, "task")
	require.NoError(t, err)

	job, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, job)

	err = q.Ack(ctx, job.ID)
	require.NoError(t, err)
}
