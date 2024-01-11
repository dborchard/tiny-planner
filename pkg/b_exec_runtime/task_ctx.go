package execution

type TaskContext struct {
	SessionID string
	TaskID    *string
	//SessionConfig      SessionConfig            // Session configuration
	//Runtime            *RuntimeEnv              // Runtime environment associated with this task context
}
