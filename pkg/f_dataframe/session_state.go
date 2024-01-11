package dataframe

import (
	"time"
	exprLogi "tiny_planner/pkg/c_expr_logical"
	execution "tiny_planner/pkg/d_exec_runtime"
	exprPhy "tiny_planner/pkg/e_expr_physcial"
)

type SessionState struct {
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

func (s SessionState) TaskContext() execution.TaskContext {
	return execution.TaskContext{
		SessionID: s.SessionID,
		TaskID:    time.Now().String(),
		Runtime:   s.RuntimeEnv,
	}
}

func (s SessionState) CreatePhysicalPlan(plan exprLogi.LogicalPlan) exprPhy.ExecutionPlan {
	return s.QueryPlanner.CreatePhysicalPlan(plan, s)
}
