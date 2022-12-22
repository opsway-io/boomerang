package boomerang

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const redisNamespace = "boomerang"

var (
	ErrUnexpectedReturnCode     = errors.New("unexpected return code from redis")
	ErrUnexpectedReturnCodeType = errors.New("unexpected return code type from redis, expected integer")
	ErrTaskAlreadyExists        = errors.New("task already exists")
	ErrTaskDoesNotExist         = errors.New("task does not exist")
	ErrTaskDataDoesNotExist     = errors.New("task data does not exist")
	ErrTaskDataInvalidFormat    = errors.New("task data has invalid format, expected JSON")
)

type Schedule interface {
	Add(ctx context.Context, task *Task, interval time.Duration, firstExecution time.Time) error
	Remove(ctx context.Context, kind string, id string) error
	RunNow(ctx context.Context, kind string, id string) error
	On(ctx context.Context, kind string, handler func(ctx context.Context, task *Task)) error
}

type TaskData struct {
	Interval time.Duration
	Data     []byte
}

type ScheduleImpl struct {
	redisClient *redis.Client
}

func NewSchedule(redisClient *redis.Client) Schedule {
	return &ScheduleImpl{
		redisClient: redisClient,
	}
}

func (s *ScheduleImpl) Add(ctx context.Context, task *Task, interval time.Duration, firstExecution time.Time) error {
	taskData, err := json.Marshal(TaskData{
		Interval: interval / time.Millisecond,
		Data:     task.Data,
	})
	if err != nil {
		return err
	}

	nextTick := firstExecution.UnixMilli()

	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local taskDataKey = KEYS[2]
		local id = ARGV[1]
		local taskData = ARGV[2]
		local score = ARGV[3]

		-- Check if the task exists

		local exists = redis.call("HEXISTS", taskDataKey, id)
		if exists == 1 then
			-- Error: task already exists
			return -1
		end

		-- Add the task to the sorted set and the task data to the hash set

		redis.call("HSETNX", taskDataKey, id, taskData)
		redis.call("ZADD", queueKey, score, id)

		-- OK
		return 0
	`)

	code, err := script.Run(
		ctx,
		s.redisClient,
		[]string{
			s.taskScheduleKey(task.Kind),
			s.taskDataKey(task.Kind),
		},
		task.ID,
		taskData,
		float64(nextTick),
	).Int()
	if err != nil {
		return err
	}

	switch code {
	case 0:
		return nil
	case -1:
		return ErrTaskAlreadyExists
	default:
		return ErrUnexpectedReturnCode
	}
}

func (s *ScheduleImpl) Remove(ctx context.Context, kind string, id string) error {
	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local taskDataKey = KEYS[2]
		local id = ARGV[1]

		-- Remove task from sorted set and check if it existed
	
		local existed = redis.call("ZREM", queueKey, id)
		if existed == 0 then
			-- Error: task does not exist
			return -1
		end

		redis.call("HDEL", taskDataKey, id)

		-- OK
		return 0
	`)

	code, err := script.Run(
		ctx,
		s.redisClient,
		[]string{
			s.taskScheduleKey(kind),
			s.taskDataKey(kind),
		},
		id,
	).Int()
	if err != nil {
		return err
	}

	switch code {
	case 0:
		return nil
	case -1:
		return ErrTaskDoesNotExist
	default:
		return ErrUnexpectedReturnCode
	}
}

func (s *ScheduleImpl) RunNow(ctx context.Context, kind string, id string) error {
	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local taskDataKey = KEYS[2]
		local id = ARGV[1]
		local score = ARGV[2]

		-- Check if the task exists

		local exists = redis.call("HEXISTS", taskDataKey, id)
		if exists == 0 then
			-- Error: task does not exist
			return -1
		end

		-- Add it to be executed now

		redis.call("ZADD", queueKey, score, id)

		-- OK
		return 0
	`)

	code, err := script.Run(
		ctx,
		s.redisClient,
		[]string{
			s.taskScheduleKey(kind),
			s.taskDataKey(kind),
		},
		id,
		float64(time.Now().UnixMilli()),
	).Int()
	if err != nil {
		return err
	}

	switch code {
	case 0:
		return nil
	case -1:
		return ErrTaskDoesNotExist
	default:
		return ErrUnexpectedReturnCode
	}
}

func (s *ScheduleImpl) On(ctx context.Context, kind string, handler func(ctx context.Context, task *Task)) error {
	queueKey := s.taskScheduleKey(kind)

	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local taskDataKey = KEYS[2]

		local now = tonumber(ARGV[1])

		-- Pop the next task from the queue

		local res = redis.call("ZPOPMIN", queueKey)
		if #res == 0 then
			-- Error: No tasks scheduled
			return { -1 }
		end

		local id = res[1]
		local score = tonumber(res[2])

		-- If the task is scheduled for more than 1 second in the future, put it back in the queue

		if score > (now + 1000) then
			redis.call("ZADD", queueKey, score, id)

			-- Error: Next task is scheduled for more than 1 second in the future
			return { -1 }
		end

		-- Get the task data

		local taskDataRaw = redis.call("HGET", taskDataKey, id)
		if taskDataRaw == nil then
			-- Error: task data does not exist
			return { -2 }
		end

		local taskData = cjson.decode(taskDataRaw)
		if taskData == nil then
			-- Error: task data has invalid format
			return { -3 }
		end

		-- Schedule the next execution
		
		local nextTick = score + taskData.Interval

		-- If the next execution is in the past, schedule it for the next interval
		if nextTick < now then
			-- Find how many intervals have passed since the last execution
			local intervals = math.floor((now - score) / taskData.Interval)
			
			-- Schedule the next execution for the next interval
			nextTick = score + (intervals * taskData.Interval) + taskData.Interval
		end

		redis.call("ZADD", queueKey, nextTick, id)

		return {0, id, score, taskDataRaw}
	`)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

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

		code, ok := resSlice[0].(int64)
		if !ok {
			return ErrUnexpectedReturnCodeType
		}

		if code == -1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}

		if code == -2 {
			return ErrTaskDataDoesNotExist
		}

		if code == -3 {
			return ErrTaskDataInvalidFormat
		}

		if code != 0 {
			return ErrUnexpectedReturnCode
		}

		id, ok := resSlice[1].(string)
		if !ok {
			return errors.New("unexpected type for id")
		}

		score, ok := resSlice[2].(int64)
		if !ok {
			return errors.New("unexpected type for score")
		}

		delta := score - time.Now().UnixMilli()
		if delta > 0 {
			time.Sleep(time.Duration(delta) * time.Millisecond)
		}

		data, ok := resSlice[3].(string)
		if !ok {
			return errors.New("unexpected type for taskDataRaw")
		}

		var taskData TaskData
		if err := json.Unmarshal([]byte(data), &taskData); err != nil {
			return err
		}

		handler(ctx, &Task{
			ID:   id,
			Kind: kind,
			Data: taskData.Data,
		})
	}
}

func (s *ScheduleImpl) taskDataKey(kind string) string {
	return fmt.Sprintf("%s:data:%s", redisNamespace, kind)
}

func (s *ScheduleImpl) taskScheduleKey(kind string) string {
	return fmt.Sprintf("%s:schedule:%s", redisNamespace, kind)
}
