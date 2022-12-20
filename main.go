package main

import (
	"context"
	"fmt"
	"time"

	"boomerang/internal/schedule"

	"github.com/go-redis/redis/v8"
)

func main() {
	ctx := context.Background()

	// connect to redis

	cli := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err := cli.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	cli.FlushAll(ctx)

	// create a queue

	q, err := schedule.NewQueue(cli)
	if err != nil {
		panic(err)
	}

	// add a task

	for i := 1; i <= 1; i++ {
		task := schedule.NewTask(
			"test",
			fmt.Sprintf("%d", i),
			time.Minute*5,
			map[string]interface{}{
				"foo": "bar",
			},
		)

		if err := q.Add(ctx, task); err != nil {
			panic(err)
		}
	}

	if err := q.On(ctx, "test", func(task *schedule.Task) {
		fmt.Printf("Executing: %v %v %v\n", task.Kind, task.ID, task.Interval)
	}); err != nil {
		panic(err)
	}

	// wait for a bit

	time.Sleep(time.Second * 10)
}
