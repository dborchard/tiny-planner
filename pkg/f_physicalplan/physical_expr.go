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
var _ Expr = BinaryExpression{}

// ----------- ColumnExpression -------------

type ColumnExpression struct {
	I int
}

func (col ColumnExpression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return input.Column(col.I), nil
}

func (col ColumnExpression) String() string {
	return "#" + strconv.Itoa(col.I)
}

// ----------- LiteralInt64Expression -------------

type LiteralInt64Expression struct {
	Value int64
}

func (lit LiteralInt64Expression) String() string {
	return strconv.FormatInt(lit.Value, 10)
}

func (lit LiteralInt64Expression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return containers.ConstVector{ArrowType: arrow.PrimitiveTypes.Int64, Value: lit.Value, Size: input.RowCount()}, nil
}

// ----------- LiteralFloat64Expression -------------

type LiteralFloat64Expression struct {
	Value float64
}

func (lit LiteralFloat64Expression) String() string {
	return strconv.FormatFloat(lit.Value, 'f', -1, 64)
}

func (lit LiteralFloat64Expression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return containers.ConstVector{ArrowType: arrow.PrimitiveTypes.Float64, Value: lit.Value, Size: input.RowCount()}, nil
}

// ----------- LiteralStringExpression -------------

type LiteralStringExpression struct {
	Value string
}

func (lit LiteralStringExpression) Evaluate(input containers.Batch) (containers.IVector, error) {
	return containers.ConstVector{ArrowType: arrow.BinaryTypes.String, Value: lit.Value, Size: input.RowCount()}, nil
}

func (lit LiteralStringExpression) String() string {
	return fmt.Sprintf("'%s'", lit.Value)
}

// ----------- BinaryExpression -------------

type BinaryExpression struct {
	l Expr
	r Expr
	BinaryExpressionEvaluator
}

type BinaryExpressionEvaluator interface {
	Evaluate(input containers.Batch) containers.IVector
	evaluate(l, r containers.IVector) containers.IVector
}

func (e BinaryExpression) Evaluate(input containers.Batch) (containers.IVector, error) {
	ll, err := e.l.Evaluate(input)
	if err != nil {
		return nil, err
	}
	rr, err := e.r.Evaluate(input)
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

func (e BinaryExpression) evaluate(l, r containers.IVector) (containers.IVector, error) {
	return e.BinaryExpressionEvaluator.evaluate(l, r), nil
}

func (e BinaryExpression) String() string {
	return e.l.String() + "+" + e.r.String()
}
