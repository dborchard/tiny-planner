package exprLogi

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
)

// ---------- Comparison ----------

func Eq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"eq", "=", l, r}
}
func Neq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"neq", "!=", l, r}
}
func Gt(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gt", ">", l, r}
}
func GtEq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gteq", ">=", l, r}
}
func Lt(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lt", "<", l, r}
}
func LtEq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lteq", "<=", l, r}
}

// ---------- BooleanBinaryExpr ----------

type BooleanBinaryExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (be BooleanBinaryExpr) ToColumnDefinition(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: be.Name,
		Type: arrow.FixedWidthTypes.Boolean,
	}
}
func (be BooleanBinaryExpr) String() string {
	return be.L.String() + " " + be.Op + " " + be.R.String()
}

func And(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"and", "AND", l, r}
}
func Or(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"or", "OR", l, r}
}

// ---------- MathExpr ----------

type MathExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (m MathExpr) ToColumnDefinition(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: m.Name,
		Type: arrow.PrimitiveTypes.Float64,
	}
}

func (m MathExpr) String() string {
	return fmt.Sprintf("%v %v %v", m.L, m.Op, m.R)
}

func Add(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"add", "+", l, r}
}

func Subtract(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"subtract", "-", l, r}
}

func Multiply(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"multiply", "*", l, r}
}

func Divide(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"divide", "/", l, r}
}

func Modulus(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"modulus", "%", l, r}
}
