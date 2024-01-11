package dataframe

import (
	"fmt"
	expr2 "tiny_planner/pkg/a_datafusion/expr"
	"tiny_planner/pkg/a_datafusion/expr/logicalplan"
	"tiny_planner/pkg/core/arrow_array"
	"tiny_planner/pkg/core/common"
	"tiny_planner/pkg/core/execution/context"
	"tiny_planner/pkg/execution"
	"tiny_planner/pkg/phyiscial_plan"
)

type DataFrame struct {
	sessionState context.SessionState
	plan         logicalplan.LogicalPlan
}

func NewDataFrame(sessionState context.SessionState, plan logicalplan.LogicalPlan) *DataFrame {
	return &DataFrame{sessionState: sessionState, plan: plan}
}

func (df *DataFrame) Project(expr []expr2.Expr) IDataFrame {
	newPlan := logicalplan.From(df.plan).Project(expr).Build()
	return &DataFrame{plan: newPlan}
}

func (df *DataFrame) Filter(predicate expr2.Expr) IDataFrame {
	newPlan := logicalplan.From(df.plan).Filter(predicate).Build()
	return &DataFrame{plan: newPlan}
}

func (df *DataFrame) Aggregate(groupBy []expr2.Expr, aggExpr []expr2.AggregateExpr) IDataFrame {
	newPlan := logicalplan.From(df.plan).Aggregate(groupBy, aggExpr).Build()
	return &DataFrame{plan: newPlan}
}

func (df *DataFrame) Explain(verbose, analyze bool) IDataFrame {
	newPlan := logicalplan.From(df.plan).Explain(verbose, analyze).Build()
	return &DataFrame{plan: newPlan}
}

func (df *DataFrame) TaskContext() execution.TaskContext {
	return df.sessionState.TaskContext()
}

func (df *DataFrame) Schema() common.DFSchema {
	return df.plan.Schema()
}

func (df *DataFrame) LogicalPlan() logicalplan.LogicalPlan {
	return df.plan
}

func (df *DataFrame) Collect() []arrow_array.RecordBatch {
	taskCtx := df.TaskContext()
	physicalPlan := df.PhysicalPlan()
	res, _ := phyiscial_plan.Collect(taskCtx, physicalPlan)
	return res
}

func (df *DataFrame) Show() {
	result := df.Collect()
	for _, batch := range result {
		fmt.Println(batch)
	}
}

func (df *DataFrame) PhysicalPlan() phyiscial_plan.ExecutionPlan {
	return df.sessionState.CreatePhysicalPlan(df.plan)
}
