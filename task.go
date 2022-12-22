package boomerang

type Task struct {
	Kind string
	ID   string
	Data []byte
}

func NewTask(kind string, id string, data []byte) *Task {
	return &Task{
		Kind: kind,
		ID:   id,
		Data: data,
	}
}
