package boomerang

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestScheduleImpl_Add(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	redisCli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})

	redisCli.FlushDB(ctx)

	schedule := NewSchedule(redisCli)

	task := NewTask(
		"test",
		"id",
		1*time.Second,
		map[string]any{
			"foo": "bar",
		},
	)

	err := schedule.Add(ctx, task)
	assert.NoError(t, err)

	err = schedule.Add(ctx, task)
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)
}

func TestScheduleImpl_Update(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	redisCli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   2,
	})

	redisCli.FlushDB(ctx)

	schedule := NewSchedule(redisCli)

	task := NewTask(
		"test",
		"id",
		1*time.Second,
		map[string]any{
			"foo": "bar",
		},
	)

	err := schedule.Update(ctx, task)
	assert.ErrorIs(t, err, ErrTaskDoesNotExist)

	err = schedule.Add(ctx, task)
	assert.NoError(t, err)

	err = schedule.Update(ctx, task)
	assert.NoError(t, err)
}

func TestScheduleImpl_Remove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	redisCli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   3,
	})

	redisCli.FlushDB(ctx)

	schedule := NewSchedule(redisCli)

	task := NewTask(
		"test",
		"id",
		1*time.Second,
		map[string]any{
			"foo": "bar",
		},
	)

	err := schedule.Remove(ctx, task.Kind, task.ID)
	assert.ErrorIs(t, err, ErrTaskDoesNotExist)

	err = schedule.Add(ctx, task)
	assert.NoError(t, err)

	err = schedule.Remove(ctx, task.Kind, task.ID)
	assert.NoError(t, err)
}
