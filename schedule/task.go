package schedule

import (
	"errors"

	"github.com/vmihailenco/msgpack/v5"
)

var ErrInvalidPayloadType = errors.New("invalid payload type")

type Task struct {
	Type    string
	Routes  []string
	Cron    string
	ID      string
	Payload any
}

// Creates a new task with the given task type, id and payload.
// One copy of the task will be sent to each route.
// For more information about the cron expression, see https://github.com/gorhill/cronexpr
func NewTask(taskType string, routes []string, cronExpr string, id string, payload any) Task {
	return Task{
		Type:    taskType,
		Routes:  routes,
		Cron:    cronExpr,
		ID:      id,
		Payload: payload,
	}
}

func MarshalTask(task Task) ([]byte, error) {
	return msgpack.Marshal(task)
}

func UnmarshalTask(data []byte) (Task, error) {
	var task Task
	if err := msgpack.Unmarshal(data, &task); err != nil {
		return task, err
	}

	return task, nil
}
