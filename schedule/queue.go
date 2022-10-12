package schedule

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

type Queue interface {
	Enqueue(ctx context.Context, task Task) error
	Consume(ctx context.Context, taskType string, routes []string, handler func(ctx context.Context, task Task) error) error
	ClearAll(ctx context.Context) error
}

type QueueImpl struct {
	queuePrefix string
	maxLen      int64
	cli         *redis.Client
}

func newQueue(cli *redis.Client, maxLen int64) Queue {
	return &QueueImpl{
		queuePrefix: "queue",
		maxLen:      maxLen,
		cli:         cli,
	}
}

func (q *QueueImpl) Enqueue(ctx context.Context, task Task) error {
	return q.cli.WithContext(ctx).XAdd(&redis.XAddArgs{
		Stream: q.streamName(task.Type, task.Routes),
		Values: map[string]interface{}{
			"id": task.ID,
		},
		MaxLen: int64(q.maxLen),
	}).Err()
}

func (q *QueueImpl) Consume(ctx context.Context, taskType string, routes []string, handler func(ctx context.Context, task Task) error) error {
	streamName := q.streamName(taskType, routes)
	consumerGroup := q.consumerGroupName(taskType, routes)

	consumerId := uuid.New().String()

	// Ensure the stream and consumer group exist
	if err := q.createStreamAndConsumerGroup(
		ctx,
		streamName,
		consumerGroup,
	); err != nil {
		return err
	}

	// Consume the stream
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			entries, err := q.cli.WithContext(ctx).XReadGroup(&redis.XReadGroupArgs{
				Group:    consumerGroup,
				Consumer: consumerId,
				Streams:  []string{streamName, ">"},
				Count:    1,
				Block:    1 * time.Second,
			}).Result()
			if err != nil {
				if err == redis.Nil {
					continue
				}

				return err
			}

			for _, entry := range entries {
				for _, message := range entry.Messages {
					handler(ctx, Task{
						ID: message.ID,
					})

					q.cli.WithContext(ctx).XAck(streamName, consumerGroup, message.ID)
				}
			}
		}
	}
}

func (q *QueueImpl) ClearAll(ctx context.Context) error {
	return q.cli.WithContext(ctx).Del(q.queuePrefix).Err()
}

func (q *QueueImpl) createStreamAndConsumerGroup(ctx context.Context, streamName string, consumerGroup string) error {
	if _, err := q.cli.WithContext(ctx).XGroupCreateMkStream(streamName, consumerGroup, "0").Result(); err != nil {
		// If the stream already exists, we can ignore the error
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}

		return err
	}

	return nil
}

func (q *QueueImpl) streamName(typ string, routes []string) string {
	return fmt.Sprintf("%s:%s:%s:stream", q.queuePrefix, typ, strings.Join(routes, ":"))
}

func (q *QueueImpl) consumerGroupName(typ string, routes []string) string {
	return fmt.Sprintf("%s:%s:%s:group", q.queuePrefix, typ, strings.Join(routes, ":"))
}
