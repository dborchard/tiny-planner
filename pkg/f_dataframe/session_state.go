package dataframe

import (
	"time"
	execution "tiny_planner/pkg/b_exec_runtime"
	exprLogi "tiny_planner/pkg/d_exprLogi"
	exprPhy "tiny_planner/pkg/e_exprPhy"
)

type SessionState struct {
	SessionID        string
	SessionStartTime time.Time
	QueryPlanner     QueryPlanner

	//LogicalOptimizer optimizer.Optimizer

	//PhysicalOptimizer physical_optimizer.PhysicalOptimizer
	//CatalogList       catalog.List
	//AggFunctions map[string]AggregateUDF
	//WindowFunctions map[string]WindowUDF
	//Config SessionConfig
	//ExecutionProps ExecutionProps
	//TableFactories map[string]TableProviderFactory
	//RuntimeEnv RuntimeEnv
}

func (s SessionState) TaskContext() execution.TaskContext {
	return execution.TaskContext{}
}

func (s SessionState) CreatePhysicalPlan(plan exprLogi.LogicalPlan) exprPhy.PhysicalPlan {
	return s.QueryPlanner.CreatePhysicalPlan(plan, s)
}
