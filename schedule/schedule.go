package schedule

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorhill/cronexpr"
)

var (
	ErrTaskAlreadyExists = errors.New("task already exists")
	ErrTaskNotExists     = errors.New("task does not exists")
)

type Scheduler interface {
	AddTask(ctx context.Context, task Task) error
	RemoveTask(ctx context.Context, id string) error
	GetTask(ctx context.Context, id string) (*Task, error)
	RunTaskNow(ctx context.Context, id string) error

	Consume(ctx context.Context, eventType string, routes []string, handler func(ctx context.Context, task Task) error) error
	Schedule(ctx context.Context) error

	ClearAll(ctx context.Context) error
}

type SchedulerImpl struct {
	scheduleSetName string
	taskSetName     string
	cli             *redis.Client
	consumerQueue   Queue
}

func NewScheduler(cli *redis.Client) Scheduler {
	return &SchedulerImpl{
		scheduleSetName: "schedule",
		taskSetName:     "task",
		cli:             cli,
		consumerQueue:   newQueue(cli, 1000000),
	}
}

func (s *SchedulerImpl) AddTask(ctx context.Context, task Task) error {
	task.Routes = unique(task.Routes)
	sort.Strings(task.Routes)

	cr, err := cronexpr.Parse(task.Cron)
	if err != nil {
		return err
	}

	taskJson, err := MarshalTask(task)
	if err != nil {
		return err
	}

	pipe := s.cli.WithContext(ctx).Pipeline()
	pipe.HSet(s.taskSetName, task.ID, taskJson)
	pipe.ZAdd(s.scheduleSetName, redis.Z{
		Score:  float64(cr.Next(time.Now()).UnixMilli()),
		Member: task.ID,
	})
	_, err = pipe.Exec()

	return err
}

func (s *SchedulerImpl) RemoveTask(ctx context.Context, id string) error {
	pipe := s.cli.WithContext(ctx).Pipeline()
	pipe.ZRem(s.scheduleSetName, id)
	pipe.HDel(s.taskSetName, id)
	res, err := pipe.Exec()
	if err != nil {
		return err
	}

	if res[0].(*redis.IntCmd).Val() == 0 {
		return ErrTaskNotExists
	}

	return nil
}

func (s *SchedulerImpl) GetTask(ctx context.Context, id string) (*Task, error) {
	res := s.cli.WithContext(ctx).HGet(s.taskSetName, id)
	if err := res.Err(); err != nil {
		return nil, err
	}

	task, err := UnmarshalTask([]byte(res.Val()))
	if err != nil {
		return nil, err
	}

	return &task, nil
}

func (s *SchedulerImpl) RunTaskNow(ctx context.Context, id string) error {
	taskResult := s.cli.WithContext(ctx).HGet(s.taskSetName, id)
	if err := taskResult.Err(); err != nil {
		return err
	}

	task, err := UnmarshalTask([]byte(taskResult.Val()))
	if err != nil {
		return err
	}

	return s.consumerQueue.Enqueue(ctx, task)
}

func (s *SchedulerImpl) Schedule(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Get next scheduled task
			res := s.cli.WithContext(ctx).ZPopMin(s.scheduleSetName, 1)
			if err := res.Err(); err != nil {
				return err
			}
			if len(res.Val()) == 0 {
				continue
			}

			// Check if task is due else wait the difference
			score := int64(res.Val()[0].Score)
			now := time.Now().UnixMilli()

			delta := score - now

			if delta > 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Duration(delta) * time.Millisecond):
				}
			}

			// Get task payload
			id, ok := res.Val()[0].Member.(string)
			if !ok {
				return errors.New("id is not string")
			}

			taskResult := s.cli.WithContext(ctx).HGet(s.taskSetName, id)
			if err := taskResult.Err(); err != nil {
				return err
			}

			task, err := UnmarshalTask([]byte(taskResult.Val()))
			if err != nil {
				return err
			}

			// Add task to worker queue
			if err := s.consumerQueue.Enqueue(ctx, task); err != nil {
				return err
			}

			// Re-schedule task
			nextScore, err := s.nextTimeStamp(task.Cron, score)
			if err != nil {
				return err
			}

			if err := s.cli.WithContext(ctx).ZAdd(s.scheduleSetName, redis.Z{
				Score:  float64(nextScore),
				Member: id,
			}).Err(); err != nil {
				return err
			}
		}
	}
}

func (s *SchedulerImpl) Consume(ctx context.Context, eventType string, routes []string, handler func(ctx context.Context, task Task) error) error {
	return s.consumerQueue.Consume(ctx, eventType, routes, handler)
}

func (s *SchedulerImpl) ClearAll(ctx context.Context) error {
	return s.cli.WithContext(ctx).FlushDB().Err()
}

func (s *SchedulerImpl) nextTimeStamp(cronExpr string, prevScore int64) (int64, error) {
	cr, err := cronexpr.Parse(cronExpr)
	if err != nil {
		return 0, err
	}

	return cr.Next(time.UnixMilli(prevScore)).UnixMilli(), nil
}
