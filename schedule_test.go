package boomerang

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var testTask1 = NewTask(
	"test",
	"id",
	10*time.Millisecond,
	map[string]any{
		"foo": "bar",
	},
)

func newSchedule(t *testing.T, ctx context.Context, db int) Schedule {
	t.Helper()

	redisCli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   db,
	})

	redisCli.FlushDB(ctx)

	return NewSchedule(redisCli)
}

func TestScheduleImpl_Add(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	schedule := newSchedule(t, ctx, 1)

	err := schedule.Add(ctx, testTask1)
	assert.NoError(t, err)

	err = schedule.Add(ctx, testTask1)
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)
}

func TestScheduleImpl_Update(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	schedule := newSchedule(t, ctx, 2)

	err := schedule.Update(ctx, testTask1)
	assert.ErrorIs(t, err, ErrTaskDoesNotExist)

	err = schedule.Add(ctx, testTask1)
	assert.NoError(t, err)

	err = schedule.Update(ctx, testTask1)
	assert.NoError(t, err)
}

func TestScheduleImpl_Remove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	schedule := newSchedule(t, ctx, 3)

	err := schedule.Remove(ctx, testTask1.Kind, testTask1.ID)
	assert.ErrorIs(t, err, ErrTaskDoesNotExist)

	err = schedule.Add(ctx, testTask1)
	assert.NoError(t, err)

	err = schedule.Remove(ctx, testTask1.Kind, testTask1.ID)
	assert.NoError(t, err)
}

func TestScheduleImpl_RunNow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	schedule := newSchedule(t, ctx, 4)

	err := schedule.RunNow(ctx, testTask1.Kind, testTask1.ID)
	assert.ErrorIs(t, err, ErrTaskDoesNotExist)

	err = schedule.Add(ctx, testTask1)
	assert.NoError(t, err)

	err = schedule.RunNow(ctx, testTask1.Kind, testTask1.ID)
	assert.NoError(t, err)
}

func TestScheduleImpl_On(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	schedule := newSchedule(t, ctx, 5)

	// Test receiving a task.

	ctxA, cancelA := context.WithTimeout(ctx, 1*time.Second)

	err := schedule.Add(ctx, testTask1)
	assert.NoError(t, err)

	err = schedule.On(ctxA, testTask1.Kind, func(ctx context.Context, task *Task) {
		cancelA()
	})

	assert.ErrorIs(t, err, context.Canceled)

	// Test never receiving a task because it is of the wrong kind.

	ctxB, cancelB := context.WithTimeout(ctx, 100*time.Millisecond)

	err = schedule.On(ctxB, "unknown", func(ctx context.Context, task *Task) {
		cancelB()
	})

	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
