package dataframe

import (
	"tiny_planner/pkg/a_datafusion/core/execution/context"
	"tiny_planner/pkg/a_datafusion/exprLogi"
	"tiny_planner/pkg/a_datafusion/exprPhy/physicalplan"
	"tiny_planner/pkg/a_datafusion/optimizer"
)

type SessionState struct {
	SessionID string

	LogicalOptimizer optimizer.Optimizer

	//QueryPlanner     QueryPlanner
	//PhysicalOptimizer physical_optimizer.PhysicalOptimizer
	//CatalogList       catalog.List
	//AggFunctions map[string]AggregateUDF
	//WindowFunctions map[string]WindowUDF
	//Config SessionConfig
	//ExecutionProps ExecutionProps
	//TableFactories map[string]TableProviderFactory
	//RuntimeEnv RuntimeEnv
}

func (s SessionState) TaskContext() context.TaskContext {
	return context.TaskContext{}
}

func (s SessionState) CreatePhysicalPlan(plan exprLogi.LogicalPlan) physicalplan.ExecutionPlan {
	return nil
}
