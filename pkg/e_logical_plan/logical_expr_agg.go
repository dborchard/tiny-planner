package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	containers "tiny_planner/pkg/i_containers"
)

// ---------- Agg----------

type AggregateExpr struct {
	Name string
	Expr Expr
}

func (e AggregateExpr) DataType(schema containers.ISchema) (arrow.DataType, error) {
	return e.Expr.DataType(schema)
}

func (e AggregateExpr) String() string {
	return fmt.Sprintf("%s(%s)", e.Name, e.String())
}

func (e AggregateExpr) ColumnsUsed(input LogicalPlan) ([]arrow.Field, error) {
	return e.Expr.ColumnsUsed(input)
}
func Sum(input Expr) AggregateExpr {
	return AggregateExpr{"SUM", input}
}

func Min(input Expr) AggregateExpr {
	return AggregateExpr{"MIN", input}
}

func Max(input Expr) AggregateExpr {
	return AggregateExpr{"MAX", input}
}

func Avg(input Expr) AggregateExpr {
	return AggregateExpr{"AVG", input}
}

func Count(input Expr) AggregateExpr {
	return AggregateExpr{"COUNT", input}
}
