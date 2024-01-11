package expr

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strconv"
	"tiny_planner/pkg/a_datafusion/expr/logicalplan"
)

type Column struct {
	name string
}

func (col Column) ToField(input logicalplan.LogicalPlan) arrow.Field {
	for _, f := range input.Schema().Fields() {
		if f.Name == col.name {
			return f
		}
	}
	panic("SQLError: No column named '$name'")

}

func (col Column) String() string {
	return "#" + col.name
}

type LiteralString struct {
	Str string
}

func (lit LiteralString) ToField(input logicalplan.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.Str,
		Type:     arrow.BinaryTypes.String,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralString) String() string {
	return fmt.Sprintf("'%s'", lit.Str)
}

type LiteralInt64 struct {
	n int64
}

func (lit LiteralInt64) ToField(input logicalplan.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.String(),
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralInt64) String() string {
	return strconv.Itoa(int(lit.n))
}

type LiteralFloat64 struct {
	n float64
}

func (lit LiteralFloat64) ToField(input logicalplan.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.String(),
		Type:     arrow.PrimitiveTypes.Float64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralFloat64) String() string {
	return strconv.FormatFloat(lit.n, 'f', -1, 64)
}

type BinaryExpr struct {
	Name string
	Op   string
	L    Expr
	R    Expr
}

func (be BinaryExpr) String() string {
	return fmt.Sprintf("%v %v %v", be.L, be.Op, be.R)
}

type BooleanBinaryExpr struct {
	Name string
	Op   string
	L    Expr
	R    Expr
}

func (be BooleanBinaryExpr) ToField(input logicalplan.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: be.Name,
		Type: arrow.FixedWidthTypes.Boolean,
	}
}

func (be BooleanBinaryExpr) String() string {
	return be.L.String() + " " + be.Op + " " + be.R.String()
}

func Eq(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"eq", "=", l, r}
}

func Neq(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"neq", "!=", l, r}
}

func Gt(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gt", ">", l, r}
}
func GtEq(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gteq", ">=", l, r}
}
func Lt(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lt", "<", l, r}
}
func LtEq(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lteq", "<=", l, r}
}

func And(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"and", "AND", l, r}
}

func Or(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"or", "OR", l, r}
}

type MathExpr struct {
	Name string
	Op   string
	L    Expr
	R    Expr
}

func (m MathExpr) String() string {
	return fmt.Sprintf("%v %v %v", m.L, m.Op, m.R)
}

func (m MathExpr) ToField(input logicalplan.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: m.Name,
		Type: arrow.PrimitiveTypes.Float64,
	}
}

func Add(l Expr, r Expr) MathExpr {
	return MathExpr{"add", "+", l, r}
}

func Subtract(l Expr, r Expr) MathExpr {
	return MathExpr{"subtract", "-", l, r}
}

func Multiply(l Expr, r Expr) MathExpr {
	return MathExpr{"multiply", "*", l, r}
}

func Divide(l Expr, r Expr) MathExpr {
	return MathExpr{"divide", "/", l, r}
}

func Modulus(l Expr, r Expr) MathExpr {
	return MathExpr{"modulus", "%", l, r}
}

type AggregateExpr struct {
	Name string
	Expr Expr
}

func (e *AggregateExpr) String() string {
	return fmt.Sprintf("%s(%s)", e.Name, e.String())
}

func (e *AggregateExpr) toField(input logicalplan.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: e.Name,
		Type: e.ToField(input).Type,
	}
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
