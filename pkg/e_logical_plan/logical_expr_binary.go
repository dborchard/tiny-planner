package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	containers "tiny_planner/pkg/i_containers"
)

// ---------- Comparison ----------

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

// ---------- BooleanBinaryExpr ----------

type BooleanBinaryExpr struct {
	Name string
	Op   string
	L    Expr
	R    Expr
}

func (be BooleanBinaryExpr) DataType(schema containers.ISchema) (arrow.DataType, error) {
	return arrow.FixedWidthTypes.Boolean, nil
}

func (be BooleanBinaryExpr) ColumnsUsed(input LogicalPlan) ([]arrow.Field, error) {
	l, err := be.L.ColumnsUsed(input)
	if err != nil {
		return nil, err
	}
	r, err := be.R.ColumnsUsed(input)
	if err != nil {
		return nil, err
	}
	return append(l, r...), nil
}
func (be BooleanBinaryExpr) String() string {
	return be.L.String() + " " + be.Op + " " + be.R.String()
}

func And(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"and", "AND", l, r}
}
func Or(l Expr, r Expr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"or", "OR", l, r}
}

// ---------- MathExpr ----------

type MathExpr struct {
	Name string
	Op   string
	L    Expr
	R    Expr
}

func (m MathExpr) DataType(schema containers.ISchema) (arrow.DataType, error) {
	return arrow.PrimitiveTypes.Float64, nil
}

func (m MathExpr) ColumnsUsed(input LogicalPlan) ([]arrow.Field, error) {
	l, err := m.L.ColumnsUsed(input)
	if err != nil {
		return nil, err
	}
	r, err := m.R.ColumnsUsed(input)
	if err != nil {
		return nil, err
	}
	return append(l, r...), nil
}

func (m MathExpr) String() string {
	return fmt.Sprintf("%v %v %v", m.L, m.Op, m.R)
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
