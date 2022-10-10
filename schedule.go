package boomerang

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
	AddTask(ctx context.Context, typ string, id string, payload any, cronExpr string, tags []string) error
	RemoveTask(ctx context.Context, id string) error
	DoesTaskExist(ctx context.Context, id string) (bool, error)
	GetTask(ctx context.Context, id string) (*Task, error)
	RunTaskNow(ctx context.Context, id string) error

	Consume(typ string, tags []string, handler func(ctx context.Context, task Task) error) error
	Schedule(ctx context.Context) error

	ClearAll(ctx context.Context) error
}

type SchedulerImpl struct {
	scheduleSetName string
	payloadSetName  string
	cli             *redis.Client
	consumerQueue   Queue
}

func NewScheduler(cli *redis.Client) Scheduler {
	return &SchedulerImpl{
		scheduleSetName: "schedule",
		payloadSetName:  "payload",
		cli:             cli,
		consumerQueue:   newQueue(cli),
	}
}

func (s *SchedulerImpl) AddTask(ctx context.Context, typ string, id string, payload any, cronExpr string, tags []string) error {
	exists, err := s.DoesTaskExist(ctx, id)
	if err != nil {
		return err
	}

	if exists {
		return ErrTaskAlreadyExists
	}

	tags = unique(tags)
	sort.Strings(tags)

	taskJson, err := MarshalTask(NewTask(id, typ, cronExpr, tags, payload))
	if err != nil {
		return err
	}

	cr, err := cronexpr.Parse(cronExpr)
	if err != nil {
		return err
	}

	pipe := s.cli.WithContext(ctx).Pipeline()
	pipe.HSet(s.payloadSetName, id, taskJson)
	pipe.ZAdd(s.scheduleSetName, redis.Z{
		Score:  float64(cr.Next(time.Now()).UnixMilli()),
		Member: id,
	})
	_, err = pipe.Exec()

	return err
}

func (s *SchedulerImpl) RemoveTask(ctx context.Context, id string) error {
	pipe := s.cli.WithContext(ctx).Pipeline()
	pipe.ZRem(s.scheduleSetName, id)
	pipe.HDel(s.payloadSetName, id)
	res, err := pipe.Exec()
	if err != nil {
		return err
	}

	if res[0].(*redis.IntCmd).Val() == 0 {
		return ErrTaskNotExists
	}

	return nil
}

func (s *SchedulerImpl) DoesTaskExist(ctx context.Context, id string) (bool, error) {
	res := s.cli.WithContext(ctx).ZScore(s.scheduleSetName, id)
	if err := res.Err(); err != nil {
		if err == redis.Nil {
			return false, nil
		}

		return false, err
	}

	return res.Val() != 0, nil
}

func (s *SchedulerImpl) GetTask(ctx context.Context, id string) (*Task, error) {
	res := s.cli.WithContext(ctx).HGet(s.payloadSetName, id)
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
	taskResult := s.cli.WithContext(ctx).HGet(s.payloadSetName, id)
	if err := taskResult.Err(); err != nil {
		return err
	}

	task, err := UnmarshalTask([]byte(taskResult.Val()))
	if err != nil {
		return err
	}

	return s.consumerQueue.Enqueue(context.Background(), task)
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
				panic(err)
			}

			if len(res.Val()) == 0 {
				continue
			}

			// Check if task is due else wait the difference
			score := int64(res.Val()[0].Score)
			now := time.Now().UnixMilli()

			delta := score - now

			if delta > 0 {
				time.Sleep(time.Duration(delta) * time.Millisecond)
			}

			// Get task payload
			id, ok := res.Val()[0].Member.(string)
			if !ok {
				return errors.New("id is not string")
			}

			taskResult := s.cli.WithContext(ctx).HGet(s.payloadSetName, id)
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
				panic(err)
			}
		}
	}
}

func (s *SchedulerImpl) Consume(typ string, tags []string, handler func(ctx context.Context, task Task) error) error {
	return s.consumerQueue.Consume(typ, tags, handler)
}

func (s *SchedulerImpl) ClearAll(ctx context.Context) error {
	pipe := s.cli.WithContext(ctx).Pipeline()
	pipe.Del(s.scheduleSetName)
	pipe.Del(s.payloadSetName)
	_, err := pipe.Exec()

	return err
}

func (s *SchedulerImpl) nextTimeStamp(cronExpr string, prevScore int64) (int64, error) {
	cr, err := cronexpr.Parse(cronExpr)
	if err != nil {
		return 0, err
	}

	return cr.Next(time.UnixMilli(prevScore)).UnixMilli(), nil
}
