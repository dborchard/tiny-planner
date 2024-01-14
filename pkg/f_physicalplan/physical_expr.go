package physicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strconv"
	containers "tiny_planner/pkg/i_containers"
)

type Expr interface {
	Evaluate(input containers.Batch) (containers.IVector, error)
	String() string
}

var _ Expr = ColumnExpression{}
var _ Expr = LiteralInt64Expression{}
var _ Expr = LiteralFloat64Expression{}
var _ Expr = LiteralStringExpression{}
var _ Expr = BooleanBinaryExpr{}

// ----------- ColumnExpression -------------

type ColumnExpression struct {
	Index int
}

func (col ColumnExpression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return input.Column(col.Index), nil
}

func (col ColumnExpression) String() string {
	return "#" + strconv.Itoa(col.Index)
}

// ----------- LiteralInt64Expression -------------

type LiteralInt64Expression struct {
	Value int64
}

func (lit LiteralInt64Expression) String() string {
	return strconv.FormatInt(lit.Value, 10)
}

func (lit LiteralInt64Expression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return containers.NewConstVector(arrow.PrimitiveTypes.Int64, input.RowCount(), lit.Value), nil
}

// ----------- LiteralFloat64Expression -------------

type LiteralFloat64Expression struct {
	Value float64
}

func (lit LiteralFloat64Expression) String() string {
	return strconv.FormatFloat(lit.Value, 'f', -1, 64)
}

func (lit LiteralFloat64Expression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return containers.NewConstVector(arrow.PrimitiveTypes.Float64, input.RowCount(), lit.Value), nil
}

// ----------- LiteralStringExpression -------------

type LiteralStringExpression struct {
	Value string
}

func (lit LiteralStringExpression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return containers.NewConstVector(arrow.BinaryTypes.String, input.RowCount(), lit.Value), nil
}

func (lit LiteralStringExpression) String() string {
	return fmt.Sprintf("'%s'", lit.Value)
}

// ----------- BooleanBinaryExpr -------------

type BooleanBinaryExpr struct {
	L  Expr
	Op string
	R  Expr
}

func (e BooleanBinaryExpr) Evaluate(input containers.Batch) (containers.IVector, error) {
	ll, err := e.L.Evaluate(input)
	if err != nil {
		return nil, err
	}
	rr, err := e.R.Evaluate(input)
	if err != nil {
		return nil, err
	}

	if ll.Len() != rr.Len() {
		return nil, fmt.Errorf("binary expression operands do not have the same length")
	}
	if ll.DataType() != rr.DataType() {
		return nil, fmt.Errorf("binary expression operands do not have the same type")
	}

	return e.evaluate(ll, rr)
}

func (e BooleanBinaryExpr) evaluate(l, r containers.IVector) (containers.IVector, error) {
	res := make([]any, 0)
	switch e.Op {
	case "=":
		for i := 0; i < l.Len(); i++ {
			if l.GetValue(i) == r.GetValue(i) {
				res = append(res, true)
			} else {
				res = append(res, false)
			}
		}
		return containers.NewVector(arrow.FixedWidthTypes.Boolean, res), nil
	default:
		return nil, fmt.Errorf("unknown binary operator: %s", e.Op)
	}
}

func (e BooleanBinaryExpr) String() string {
	return e.L.String() + "+" + e.R.String()
}
