package exprLogi

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strconv"
)

type LogicalExpr interface {
	ToField(input LogicalPlan) arrow.Field
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

func (col Column) ToField(input LogicalPlan) arrow.Field {
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

func (expr Alias) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: expr.Alias,
		Type: expr.Expr.ToField(input).Type,
	}
}

func (expr Alias) String() string {
	return fmt.Sprintf("%s as %s", expr.Expr.String(), expr.Alias)
}

// ---------- BooleanBinaryExpr ----------

type BooleanBinaryExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (be BooleanBinaryExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: be.Name,
		Type: arrow.FixedWidthTypes.Boolean,
	}
}
func (be BooleanBinaryExpr) String() string {
	return be.L.String() + " " + be.Op + " " + be.R.String()
}

// ---------- MathExpr ----------

type MathExpr struct {
	Name string
	Op   string
	L    LogicalExpr
	R    LogicalExpr
}

func (m MathExpr) String() string {
	return fmt.Sprintf("%v %v %v", m.L, m.Op, m.R)
}

func (m MathExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: m.Name,
		Type: arrow.PrimitiveTypes.Float64,
	}
}

// ---------- Agg----------

type AggregateExpr struct {
	Name string
	Expr LogicalExpr
}

func (e AggregateExpr) String() string {
	return fmt.Sprintf("%s(%s)", e.Name, e.String())
}

func (e AggregateExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: e.Name,
		Type: e.Expr.ToField(input).Type,
	}
}

// ---------- Literals ----------

type LiteralString struct {
	Str string
}

func (lit LiteralString) ToField(input LogicalPlan) arrow.Field {
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
	N int64
}

func (lit LiteralInt64) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name:     lit.String(),
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}
}

func (lit LiteralInt64) String() string {
	return strconv.Itoa(int(lit.N))
}

type LiteralFloat64 struct {
	n float64
}

func (lit LiteralFloat64) ToField(input LogicalPlan) arrow.Field {
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
