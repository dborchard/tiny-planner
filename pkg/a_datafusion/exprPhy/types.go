package exprPhy

import (
	"fmt"
	"tiny_planner/pkg/a_datafusion/common"
	"tiny_planner/pkg/a_datafusion/core/execution/context"
)

func Collect(taskContext context.TaskContext, plan PhysicalPlan) ([]common.Batch, error) {
	return nil, fmt.Errorf("not implemented")
}
