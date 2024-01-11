package physicalplan

import (
	"fmt"
	"tiny_planner/pkg/a_datafusion/common"
	"tiny_planner/pkg/a_datafusion/core/execution/context"
)

type ExecutionPlan interface {
}

func Collect(taskContext context.TaskContext, plan ExecutionPlan) ([]common.Batch, error) {
	return nil, fmt.Errorf("not implemented")
}
