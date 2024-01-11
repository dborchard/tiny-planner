package exprPhy

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strconv"
	"tiny_planner/pkg/a_datafusion/common"
)

type Expression interface {
	Evaluate(input common.Batch) common.Vector
	String() string
}

// ----------- ColumnExpression -------------

type ColumnExpression struct {
	i int
}

func (col ColumnExpression) Evaluate(input common.Batch) common.Vector {
	return input.Field(col.i)
}

func (col ColumnExpression) String() string {
	return "#" + strconv.Itoa(col.i)
}

// ----------- LiteralInt64Expression -------------

type LiteralInt64Expression struct {
	value int64
}

func (lit LiteralInt64Expression) String() string {
	return strconv.FormatInt(lit.value, 10)
}

func (lit LiteralInt64Expression) Evaluate(input common.Batch) common.Vector {
	return common.LiteralValueVector{ArrowType: arrow.PrimitiveTypes.Int64, Value: lit.value, Size: input.RowCount()}
}

// ----------- LiteralFloat64Expression -------------

type LiteralFloat64Expression struct {
	value float64
}

func (lit LiteralFloat64Expression) String() string {
	return strconv.FormatFloat(lit.value, 'f', -1, 64)
}

func (lit LiteralFloat64Expression) Evaluate(input common.Batch) common.Vector {
	return common.LiteralValueVector{ArrowType: arrow.PrimitiveTypes.Float64, Value: lit.value, Size: input.RowCount()}
}

// ----------- LiteralStringExpression -------------

type LiteralStringExpression struct {
	value string
}

func (lit LiteralStringExpression) Evaluate(input common.Batch) common.Vector {
	return common.LiteralValueVector{ArrowType: arrow.BinaryTypes.String, Value: lit.value, Size: input.RowCount()}
}

func (lit LiteralStringExpression) String() string {
	return fmt.Sprintf("'%s'", lit.value)
}

// ----------- BinaryExpression -------------

type BinaryExpression struct {
	l Expression
	r Expression
	BinaryExpressionEvaluator
}

type BinaryExpressionEvaluator interface {
	Evaluate(input common.Batch) common.Vector
	evaluate(l, r common.Vector) common.Vector
}

func (e BinaryExpression) Evaluate(input common.Batch) common.Vector {
	ll := e.l.Evaluate(input)
	rr := e.r.Evaluate(input)
	if ll.Len() != rr.Len() {
		panic("Binary expression operands do not have the same size")
	}
	if ll.DataType() != rr.DataType() {
		panic("Binary expression operands do not have the same type")
	}
	return e.evaluate(ll, rr)
}

func (e BinaryExpression) evaluate(l, r common.Vector) common.Vector {
	return e.BinaryExpressionEvaluator.evaluate(l, r)
}

//----------- MathExpression -------------

type MathExpression struct {
	l Expression
	r Expression
	MathExpressionEvaluator
}

type MathExpressionEvaluator interface {
	Expression
	evaluate(l any, r any, arrowType arrow.DataType) any
}

func (e MathExpression) Evaluate(l common.Vector, r common.Vector) common.Vector {
	values := make([]any, l.Len())
	for i := 0; i < l.Len(); i++ {
		value := e.evaluate(l.GetValue(i), r.GetValue(i), l.DataType())
		values[i] = value
	}

	return common.NewArray(l.DataType(), l.Len(), values)
}

type AddExpression struct {
	MathExpression
}

func (e AddExpression) Evaluate(l any, r any, arrowType arrow.DataType) any {
	switch arrowType {
	case common.Int64:
		return l.(int64) + r.(int64)
	case common.Int32:
		return l.(int32) + r.(int32)
	case common.Int16:
		return l.(int16) + r.(int16)
	case common.Int8:
		return l.(int8) + r.(int8)
	case common.Float64:
		return l.(float64) + r.(float64)
	case common.Float32:
		return l.(float32) + r.(float32)
	default:
		panic("unsupported type")
	}
}

func (e AddExpression) String() string {
	return e.l.String() + "+" + e.r.String()
}

// ----------- AggregateExpression -------------

type AggregateExpression interface {
	InputExpression() Expression
	CreateAccumulator() Accumulator
}

type Accumulator interface {
	Accumulate(value any)
	FinalValue() any
}

type MaxExpression struct {
	expr Expression
}

func (e MaxExpression) InputExpression() Expression {
	return e.expr
}

func (e MaxExpression) CreateAccumulator() Accumulator {
	return &MaxAccumulator{}
}

func (e MaxExpression) String() string {
	return "MAX(" + e.expr.String() + ")"
}

type MaxAccumulator struct {
	value any
}

func (a *MaxAccumulator) Accumulate(value any) {
	if a.value == nil {
		a.value = value
		return
	}
	switch value.(type) {
	case int8:
		if a.value.(int8) < value.(int8) {
			a.value = value
		}
	case int16:
		if a.value.(int16) < value.(int16) {
			a.value = value
		}
	case int32:
		if a.value.(int32) < value.(int32) {
			a.value = value
		}
	case int64:
		if a.value.(int64) < value.(int64) {
			a.value = value
		}
	case float64:
		if a.value.(float64) < value.(float64) {
			a.value = value
		}
	case float32:
		if a.value.(float32) < value.(float32) {
			a.value = value
		}
	default:
		panic("unsupported type")
	}
}

func (a *MaxAccumulator) FinalValue() any {
	return a.value
}
