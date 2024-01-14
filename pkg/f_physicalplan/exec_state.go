package physicalplan

import (
	"context"
	"time"
	logicalplan "tiny_planner/pkg/e_logical_plan"
	"tiny_planner/pkg/g_exec_runtime"
)

type ExecState struct {
	SessionID        string
	SessionStartTime time.Time
	QueryPlanner     QueryPlanner
	RuntimeEnv       *execution.RuntimeEnv

	//LogicalOptimizer optimizer.Optimizer
	//PhysicalOptimizer physical_optimizer.PhysicalOptimizer

	//CatalogList       catalog.List
	//AggFunctions map[string]AggregateUDF
	//WindowFunctions map[string]WindowUDF
	//Config SessionConfig
	//ExecutionProps ExecutionProps
	//TableFactories map[string]TableProviderFactory

}

func (s ExecState) TaskContext() execution.TaskContext {
	return execution.TaskContext{
		SessionID: s.SessionID,
		TaskID:    time.Now().String(),
		Runtime:   s.RuntimeEnv,
		Ctx:       context.Background(),
	}
}

func (s ExecState) CreatePhysicalPlan(plan logicalplan.LogicalPlan) (PhysicalPlan, error) {
	return s.QueryPlanner.CreatePhyPlan(plan, s)
}
