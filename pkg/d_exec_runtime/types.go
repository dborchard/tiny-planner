package execution

type TaskContext struct {
	SessionID string
	TaskID    string
	Runtime   *RuntimeEnv
}
