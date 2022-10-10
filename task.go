package boomerang

import (
	"errors"

	"github.com/mailru/easyjson"
)

var ErrInvalidPayloadType = errors.New("invalid payload type")

//easyjson:json
type Task struct {
	ID      string
	Type    string
	Cron    string
	Tags    []string
	Payload any
}

func NewTask(id string, typ string, cron string, tags []string, payload any) Task {
	return Task{
		ID:      id,
		Type:    typ,
		Cron:    cron,
		Tags:    tags,
		Payload: payload,
	}
}

func MarshalTask(task Task) ([]byte, error) {
	return easyjson.Marshal(task)
}

func UnmarshalTask(data []byte) (Task, error) {
	var task Task
	if err := easyjson.Unmarshal(data, &task); err != nil {
		return Task{}, err
	}

	return task, nil
}
