package exprPhy

import (
	containers "tiny_planner/pkg/a_containers"
	execution "tiny_planner/pkg/b_exec_runtime"
)

func Collect(taskContext execution.TaskContext, plan PhysicalPlan) ([]containers.Batch, error) {
	return plan.Execute(), nil
}
