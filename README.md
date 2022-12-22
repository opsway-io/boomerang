# Boomerang 🪃

Simple distributed recurring task scheduler for golang implemented on top of [redis](https://redis.io/).

[![Test](https://github.com/opsway-io/boomerang/actions/workflows/test.yaml/badge.svg)](https://github.com/opsway-io/boomerang/actions/workflows/test.yaml)

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

    cli := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    sch := boomerang.NewSchedule(cli)

    task := boomerang.NewTask(
        "greeter",
        "some-unique-id",
        "Hello!",
    )

    // Schedule task for execution every second starting from now
    if err := sch.Add(ctx, task, time.Second, time.Now()); err != nil {
        panic(err)
    }

    sch.On(ctx, "greeter", func(ctx context.Context, task *boomerang.Task) {
        fmt.Println(task.Data)
    })
}
```
