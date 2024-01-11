package exprLogi

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strconv"
)

type LogicalExpr interface {
	ToColumnDefinition(input LogicalPlan) arrow.Field
	String() string
}

var _ LogicalExpr = Column{}
var _ LogicalExpr = Alias{}

var _ LogicalExpr = BooleanBinaryExpr{}
var _ LogicalExpr = MathExpr{}
var _ LogicalExpr = AggregateExpr{}

var _ LogicalExpr = LiteralString{}
var _ LogicalExpr = LiteralInt64{}
var _ LogicalExpr = LiteralFloat64{}

// ---------- Column ----------

type Column struct {
	Name string
}

func (col Column) ToColumnDefinition(input LogicalPlan) arrow.Field {
	for _, f := range input.Schema().Fields() {
		if f.Name == col.Name {
			return f
		}
	}
	panic("SQLError: No column named '$name'")
}

func (col Column) String() string {
	return "#" + col.Name
}

// ---------- Alias ----------

type Alias struct {
	Expr  LogicalExpr
	Alias string
}

func (expr Alias) ToColumnDefinition(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: expr.Alias,
		Type: expr.Expr.ToColumnDefinition(input).Type,
	}
}

func (expr Alias) String() string {
	return fmt.Sprintf("%s as %s", expr.Expr.String(), expr.Alias)
}

// ---------- Literals ----------

type LiteralString struct {
	Val string
}

func (lit LiteralString) ToColumnDefinition(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.Val,
		Type:     arrow.BinaryTypes.String,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralString) String() string {
	return fmt.Sprintf("'%s'", lit.Val)
}

type LiteralInt64 struct {
	Val int64
}

func (lit LiteralInt64) ToColumnDefinition(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.String(),
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralInt64) String() string {
	return strconv.Itoa(int(lit.Val))
}

type LiteralFloat64 struct {
	Val float64
}

func (lit LiteralFloat64) ToColumnDefinition(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.String(),
		Type:     arrow.PrimitiveTypes.Float64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralFloat64) String() string {
	return strconv.FormatFloat(lit.Val, 'f', -1, 64)
}
