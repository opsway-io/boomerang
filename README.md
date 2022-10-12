# Boomerang 🪃

Recurring tasks scheduler for golang implemented on top of [redis](https://redis.io/).

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/opsway-io/boomerang"
)

func main() {
    ctx := context.Background()

    // Create redis client
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
        DB:   0,
    })

    // Create schedule
    schedule := boomerang.NewSchedule(client)

    // Create task
    task := boomerang.NewTask(
        "task_type",
        []string{"a_route"},
        "1/5 * * * * * *", // Run every 5th second
        "some_unique_id",
        "task_payload",
    )

    if err := schedule.AddTask(ctx, task); err != nil {
        panic(err)
    }

    // Consume tasks
    go schedule.Consume(ctx, "task_type", []string{"a_route"}  func(ctx context.Context, task *boomerang.Task) error {
        fmt.Println(task)
        return nil
    })

    // Start the scheduler
    if err := schedule.Run(ctx); err != nil {
        panic(err)
    }
}

```

## Important notes

- Boomerang expects to have a redis database by itself. Sharing a database with another application might cause conflicts in keys.
- The boomerang scheduler **MUST** be gracefully shutdown otherwise scheduled tasks in-flight WILL be lost. This is not ideal and might change in the future.

