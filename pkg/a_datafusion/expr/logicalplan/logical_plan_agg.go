package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"tiny_planner/pkg/a_datafusion/expr"
	"tiny_planner/pkg/core/common"
)

type Aggregate struct {
	Input         LogicalPlan
	GroupExpr     []expr.Expr
	AggregateExpr []expr.AggregateExpr
}

func (a Aggregate) Schema() common.DFSchema {
	var fields []arrow.Field
	for _, e := range a.GroupExpr {
		fields = append(fields, e.ToField(a.Input))
	}
	for _, e := range a.AggregateExpr {
		fields = append(fields, e.toField(a.Input))
	}
	return common.DFSchema{Schema: arrow.NewSchema(fields, nil)}
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Input}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%s, aggregateExpr=%s", a.GroupExpr, a.AggregateExpr)
}
