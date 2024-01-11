package execution

import (
	"tiny_planner/pkg/a_datafusion/expr/logicalplan"
)

type TaskContext struct {
	SessionID string  // Session Id
	TaskID    *string // Optional Task Identifier
	//SessionConfig      SessionConfig            // Session configuration
	ScalarFunctions map[string]*logicalplan.ScalarUDF // Scalar functions associated with this task context
	//Runtime            *RuntimeEnv              // Runtime environment associated with this task context
}
