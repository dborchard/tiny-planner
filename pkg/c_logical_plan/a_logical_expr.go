package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strconv"
	containers "tiny_planner/pkg/g_containers"
)

type Expr interface {
	// DataType returns the data type of the expression. It returns error as well.
	DataType(schema containers.ISchema) (arrow.DataType, error)

	// ColumnsUsed returns the columns used in the expression.
	//TODO: replace it with ColumnsUsedExprs() []Expr
	ColumnsUsed(input LogicalPlan) []arrow.Field
	String() string
}

var _ Expr = Column{}
var _ Expr = Alias{}

var _ Expr = BooleanBinaryExpr{}
var _ Expr = MathExpr{}
var _ Expr = AggregateExpr{}

var _ Expr = LiteralString{}
var _ Expr = LiteralInt64{}
var _ Expr = LiteralFloat64{}

// ---------- Column ----------

type Column struct {
	Name string
}

func (col Column) DataType(schema containers.ISchema) (arrow.DataType, error) {
	for _, f := range schema.Fields() {
		if f.Name == col.Name {
			return f.Type, nil
		}
	}
	return nil, fmt.Errorf("column %s not found", col.Name)
}

func (col Column) ColumnsUsed(input LogicalPlan) []arrow.Field {
	schema := input.Schema()
	for _, f := range schema.Fields() {
		if f.Name == col.Name {
			return []arrow.Field{f}
		}
	}
	panic(fmt.Sprintf("column %s not found", col.Name))
	return []arrow.Field{}
}

func (col Column) String() string {
	return "#" + col.Name
}

// ---------- Alias ----------

type Alias struct {
	Expr  Expr
	Alias string
}

func (expr Alias) DataType(schema containers.ISchema) (arrow.DataType, error) {
	return expr.Expr.DataType(schema)
}

func (expr Alias) ColumnsUsed(input LogicalPlan) []arrow.Field {
	return expr.Expr.ColumnsUsed(input)
}

func (expr Alias) String() string {
	return fmt.Sprintf("%s as %s", expr.Expr.String(), expr.Alias)
}

// ---------- Literals ----------

type LiteralString struct {
	Val string
}

func (lit LiteralString) DataType(schema containers.ISchema) (arrow.DataType, error) {
	return arrow.BinaryTypes.String, nil
}

func (lit LiteralString) ColumnsUsed(input LogicalPlan) []arrow.Field {
	return nil
}

func (lit LiteralString) String() string {
	return fmt.Sprintf("'%s'", lit.Val)
}

type LiteralInt64 struct {
	Val int64
}

func (lit LiteralInt64) DataType(schema containers.ISchema) (arrow.DataType, error) {
	return arrow.PrimitiveTypes.Int64, nil
}

func (lit LiteralInt64) ColumnsUsed(input LogicalPlan) []arrow.Field {
	return nil
}

func (lit LiteralInt64) String() string {
	return strconv.Itoa(int(lit.Val))
}

type LiteralFloat64 struct {
	Val float64
}

func (lit LiteralFloat64) DataType(schema containers.ISchema) (arrow.DataType, error) {
	return arrow.PrimitiveTypes.Float64, nil
}

func (lit LiteralFloat64) ColumnsUsed(input LogicalPlan) []arrow.Field {
	return nil
}

func (lit LiteralFloat64) String() string {
	return strconv.FormatFloat(lit.Val, 'f', -1, 64)
}
