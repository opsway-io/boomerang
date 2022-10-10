package boomerang

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redis"
)

func TestNewScheduler(t *testing.T) {
	ctx := context.Background()

	cli := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	sch := NewScheduler(cli)

	fmt.Println("Clearing all tasks")

	err := sch.ClearAll(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("Spawning schedulers")

	for i := 0; i < 10; i++ {
		go sch.Schedule(context.Background())
	}

	fmt.Println("Adding tasks")

	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("%d", i)
		err := sch.AddTask(
			ctx,
			"task:type",
			id,
			map[string]any{
				"foo": "bar",
			},
			"* * * * * * *",
			[]string{
				"eu-central-1",
			},
		)
		if err != nil {
			panic(err)
		}
	}

	select {}
}
