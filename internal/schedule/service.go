package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Task struct {
	Kind     string
	ID       string
	Interval time.Duration
	Data     any
}

func NewTask(kind, id string, interval time.Duration, data any) *Task {
	return &Task{
		Kind:     kind,
		ID:       id,
		Interval: interval,
		Data:     data,
	}
}

type Queue interface {
	Add(ctx context.Context, task *Task) error
	Remove(ctx context.Context, kind string, id string) error
	RunNow(ctx context.Context, kind string, id string) error
	On(ctx context.Context, kind string, handler func(task *Task)) error
}

type TaskData struct {
	Interval time.Duration
	Data     any
}

type QueueImpl struct {
	redisClient *redis.Client
}

func NewQueue(redisClient *redis.Client) (Queue, error) {
	return &QueueImpl{
		redisClient: redisClient,
	}, nil
}

func (s *QueueImpl) Add(ctx context.Context, task *Task) error {
	// Marshal the task data

	payload, err := json.Marshal(TaskData{
		Interval: task.Interval / time.Millisecond,
		Data:     task.Data,
	})
	if err != nil {
		return err
	}

	// Find the next execution time

	now := time.Now()
	nextTick := now.Add(task.Interval).UnixMilli()

	// Execute redis script to
	// add the task to the sorted set
	// and the task data to the hash set

	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local payloadKey = KEYS[2]
		local id = ARGV[1]
		local payload = ARGV[2]
		local score = ARGV[3]

		redis.call("HSETNX", payloadKey, id, payload)
		redis.call("ZADD", queueKey, score, id)

		return 1
	`)

	if err := script.Run(
		ctx,
		s.redisClient,
		[]string{
			s.taskQueueKey(task.Kind),
			s.taskDataKey(task.Kind),
		},
		task.ID,
		payload,
		float64(nextTick),
	).Err(); err != nil {
		return err
	}

	return nil
}

func (s *QueueImpl) Remove(ctx context.Context, kind string, id string) error {
	// Remove the task from the sorted set and the task data from the hash set

	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local payloadKey = KEYS[2]
		local id = ARGV[1]

		redis.call("HDEL", payloadKey, id)
		redis.call("ZREM", queueKey, id)

		return 1
	`)

	if err := script.Run(
		ctx,
		s.redisClient,
		[]string{
			s.taskQueueKey(kind),
			s.taskDataKey(kind),
		},
		id,
	).Err(); err != nil {
		return err
	}

	return nil
}

func (s *QueueImpl) RunNow(ctx context.Context, kind string, id string) error {
	// Add the task to the sorted set with a score of now

	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local id = ARGV[1]
		local score = ARGV[2]

		redis.call("ZADD", queueKey, score, id)

		return 1
	`)

	if err := script.Run(
		ctx,
		s.redisClient,
		[]string{
			s.taskQueueKey(kind),
		},
		id,
		float64(time.Now().UnixMilli()),
	).Err(); err != nil {
		return err
	}

	return nil
}

func (s *QueueImpl) On(ctx context.Context, kind string, handler func(task *Task)) error {
	queueKey := s.taskQueueKey(kind)

	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local taskDataKey = KEYS[2]

		local now = tonumber(ARGV[1])

		local res = redis.call("ZPOPMIN", queueKey)
		if #res == 0 then
			return { -1 }
		end

		local id = res[1]
		local score = tonumber(res[2])

		if score > (now + 1000) then
			redis.call("ZADD", queueKey, score, id)
			return { -1 }
		end

		local taskDataRaw = redis.call("HGET", taskDataKey, id)
		if taskDataRaw == nil then
			return { -1 }
		end

		local taskData = cjson.decode(taskDataRaw)
		if taskData == nil then
			return { -1 }
		end

		local nextTick = score + taskData.Interval

		redis.call("ZADD", queueKey, nextTick, id)

		return {id, score, taskDataRaw}
	`)

	for {
		taskDataKey := s.taskDataKey(kind)

		res := script.Run(
			ctx,
			s.redisClient,
			[]string{
				queueKey,
				taskDataKey,
			},
			time.Now().UnixMilli(),
		)
		if err := res.Err(); err != nil {
			return err
		}

		resSlice, err := res.Slice()
		if err != nil {
			return err
		}

		if len(resSlice) != 3 {
			time.Sleep(1 * time.Second)

			continue
		}

		id, ok := resSlice[0].(string)
		if !ok {
			return errors.New("unexpected type for id")
		}

		score, ok := resSlice[1].(int64)
		if !ok {
			return errors.New("unexpected type for score")
		}

		delta := score - time.Now().UnixMilli()
		if delta > 0 {
			time.Sleep(time.Duration(delta) * time.Millisecond)
		}

		taskDataRaw, ok := resSlice[2].(string)
		if !ok {
			return errors.New("unexpected type for taskDataRaw")
		}

		var taskData TaskData
		if err := json.Unmarshal([]byte(taskDataRaw), &taskData); err != nil {
			return err
		}

		handler(&Task{
			ID:       id,
			Kind:     kind,
			Interval: time.Duration(taskData.Interval) * time.Millisecond,
			Data:     taskData.Data,
		})
	}
}

func (s *QueueImpl) taskDataKey(kind string) string {
	return fmt.Sprintf("data:%s", kind)
}

func (s *QueueImpl) taskQueueKey(kind string) string {
	return fmt.Sprintf("queue:%s", kind)
}
