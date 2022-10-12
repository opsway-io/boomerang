package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/go-redis/redis"
	"github.com/opsway-io/boomerang/schedule"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	cli := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	sch := schedule.NewScheduler(cli)
	if err := sch.ClearAll(ctx); err != nil {
		panic(err)
	}

	// Add task handler

	for i := 0; i < 100; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()

			err := sch.Consume(
				ctx,
				"http_probe",
				[]string{
					"eu-central-1",
					"eu-central-2",
				},
				func(ctx context.Context, task schedule.Task) error {
					fmt.Printf("Got task: %s\n", task.ID)

					return nil
				},
			)
			if err != nil {
				panic(err)
			}
		}()
	}

	// Start schedulers

	for i := 0; i < 20; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()

			if err := sch.Schedule(ctx); err != nil {
				panic(err)
			}
		}()
	}

	// Add some tasks

	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%d", i)

		task := schedule.NewTask(
			"http_probe",
			[]string{
				"eu-central-1",
				"eu-central-2",
			},
			"1/5 * * * * * *",
			id,
			"payload",
		)

		if err := sch.AddTask(ctx, task); err != nil {
			panic(err)
		}
	}

	// Wait for interrupt

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan

	fmt.Println("Got interrupt, shutting down")

	cancel()

	wg.Wait()
}
