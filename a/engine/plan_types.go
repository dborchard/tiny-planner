package engine

import "github.com/apache/arrow/go/v12/arrow"

type LogicalPlan interface {
	Schema() Schema
	Children() []LogicalPlan
	String() string
}

type LogicalExpr interface {
	ToField(input LogicalPlan) arrow.Field
	String() string
}

var _ LogicalExpr = Column{}
var _ LogicalExpr = LiteralString{}
var _ LogicalExpr = LiteralInt64{}
var _ LogicalExpr = LiteralFloat64{}
