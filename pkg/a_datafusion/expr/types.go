package expr

import (
	"github.com/apache/arrow/go/v12/arrow"
	"tiny_planner/pkg/a_datafusion/expr/logicalplan"
)

type Expr interface {
	ToField(input logicalplan.LogicalPlan) arrow.Field
	String() string
}

var _ Expr = Column{}
var _ Expr = LiteralString{}
var _ Expr = LiteralInt64{}
var _ Expr = LiteralFloat64{}
