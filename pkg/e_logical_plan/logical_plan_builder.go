package logicalplan

import (
	datasource "tiny_planner/pkg/h_storage_engine"
)

type Builder struct {
	plan LogicalPlan
}

func NewBuilder() Builder {
	return Builder{}
}

func From(plan LogicalPlan) Builder {
	return Builder{plan: plan}
}

func (b Builder) Scan(path string, source datasource.TableReader, proj []string) Builder {
	return Builder{plan: Scan{Path: path, Source: source, Projection: proj}}
}

func (b Builder) Project(expr ...Expr) Builder {
	return Builder{plan: Projection{b.plan, expr}}
}

func (b Builder) Filter(pred Expr) Builder {
	return Builder{plan: Selection{b.plan, pred}}
}

func (b Builder) Aggregate(groupBy []Expr, aggExpr []AggregateExpr) Builder {
	return Builder{plan: Aggregate{b.plan, groupBy, aggExpr}}
}

func (b Builder) Build() (LogicalPlan, error) {
	if err := Validate(b.plan); err != nil {
		return nil, err
	}
	return b.plan, nil
}

func Validate(plan LogicalPlan) error {
	return nil
}
