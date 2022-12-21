package boomerang

import (
	"context"
	"fmt"
	"time"

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

	sch := NewSchedule(cli)

	// add a task

	for i := 1; i <= 10; i++ {
		task := NewTask(
			"test",
			fmt.Sprintf("%d", i),
			time.Second*5,
			map[string]interface{}{
				"foo": "bar",
			},
		)

		if err := sch.Add(ctx, task); err != nil {
			panic(err)
		}
	}

	if err := sch.On(ctx, "test", func(ctx context.Context, task *Task) {
		fmt.Printf("Executing: %v %v %v\n", task.Kind, task.ID, task.Interval)
	}); err != nil {
		panic(err)
	}
}
