package logicalplan

import (
	"tiny_planner/pkg/a_datafusion/expr"
)

type LogicalPlanBuilder struct {
	plan LogicalPlan
}

func From(plan LogicalPlan) LogicalPlanBuilder {
	return LogicalPlanBuilder{plan: plan}
}

func (b LogicalPlanBuilder) Project(expr []expr.Expr) LogicalPlanBuilder {
	return b
}

func (b LogicalPlanBuilder) Filter(predicate expr.Expr) LogicalPlanBuilder {
	return b
}

func (b LogicalPlanBuilder) Aggregate(groupBy []expr.Expr, aggExpr []expr.AggregateExpr) LogicalPlanBuilder {
	return b
}

func (b LogicalPlanBuilder) Explain(verbose bool, analyze bool) LogicalPlanBuilder {
	return b
}

func (b LogicalPlanBuilder) Build() LogicalPlan {
	return b.plan
}
