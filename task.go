package boomerang

import "time"

type Task struct {
	Kind     string
	ID       string
	Interval time.Duration
	Data     any
}

func NewTask(kind, id string, interval time.Duration, data any) *Task {
	return &Task{
		Kind:     kind,
		ID:       id,
		Interval: interval,
		Data:     data,
	}
}
