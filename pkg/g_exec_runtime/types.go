package execution

import "context"

type TaskContext struct {
	SessionID string
	TaskID    string
	Runtime   *RuntimeEnv
	Ctx       context.Context
}
