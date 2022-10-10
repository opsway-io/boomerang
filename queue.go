package boomerang

import (
	"context"
	"fmt"

	"github.com/go-redis/redis"
)

type Queue interface {
	Enqueue(ctx context.Context, task Task) error
	Consume(typ string, tags []string, handler func(ctx context.Context, task Task) error) error
}

type QueueImpl struct {
	queuePrefix string
	cli         *redis.Client
}

func newQueue(cli *redis.Client) Queue {
	return &QueueImpl{
		queuePrefix: "queue",
		cli:         cli,
	}
}

func (q *QueueImpl) Enqueue(ctx context.Context, task Task) error {
	fmt.Printf("Enqueueing task %s of type %s with tags %v\n", task.ID, task.Type, task.Payload)

	// TODO: implement

	return nil
}

func (q *QueueImpl) Consume(typ string, tags []string, handler func(ctx context.Context, task Task) error) error {
	// TODO: implement

	return nil
}
