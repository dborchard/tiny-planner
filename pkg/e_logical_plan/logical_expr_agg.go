package exprLogi

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
)

// ---------- Agg----------

type AggregateExpr struct {
	Name string
	Expr LogicalExpr
}

func (e AggregateExpr) String() string {
	return fmt.Sprintf("%s(%s)", e.Name, e.String())
}

func (e AggregateExpr) ToColumnDefinition(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: e.Name,
		Type: e.Expr.ToColumnDefinition(input).Type,
	}
}
func Sum(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"SUM", input}
}

func Min(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MIN", input}
}

func Max(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MAX", input}
}

func Avg(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"AVG", input}
}

func Count(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"COUNT", input}
}
