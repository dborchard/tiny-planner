package dataframe

import (
	"tiny_planner/pkg/a_datafusion/expr"
	logical_plan2 "tiny_planner/pkg/a_datafusion/expr/logicalplan"
	"tiny_planner/pkg/core/arrow_array"
	"tiny_planner/pkg/core/common"
	"tiny_planner/pkg/phyiscial_plan"
)

type IDataFrame interface {
	Project(expr []expr.Expr) IDataFrame
	Filter(expr expr.Expr) IDataFrame
	Aggregate(groupBy []expr.Expr, aggregateExpr []expr.AggregateExpr) IDataFrame
	Explain(verbose, analyze bool) IDataFrame

	Schema() common.DFSchema
	Collect() []arrow_array.RecordBatch
	Show()

	LogicalPlan() logical_plan2.LogicalPlan
	PhysicalPlan() phyiscial_plan.ExecutionPlan
}
