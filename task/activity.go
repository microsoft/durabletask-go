package task

import (
	"github.com/microsoft/durabletask-go/internal/protos"
)

// ActivityContext is the context parameter type for activity implementations.
type ActivityContext interface {
	GetInput(resultPtr any) error
}

type activityContext struct {
	TaskID int32
	Name   string

	rawInput []byte
}

// Activity is the functional interface for activity implementations.
type Activity func(ctx ActivityContext) (any, error)

func newTaskActivityContext(taskID int32, ts *protos.TaskScheduledEvent) *activityContext {
	return &activityContext{
		TaskID:   taskID,
		Name:     ts.Name,
		rawInput: []byte(ts.Input.GetValue()),
	}
}

// GetInput unmarshals the serialized activity input and saves the result into [v].
func (ctx *activityContext) GetInput(v any) error {
	return unmarshalData(ctx.rawInput, v)
}
