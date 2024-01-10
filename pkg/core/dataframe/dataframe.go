package dataframe

import (
	"fmt"
	"tiny_planner/a/engine"

	"github.com/apache/arrow/go/v12/arrow"
)

type DataFrame interface {
	Project(expr []engine.LogicalExpr) DataFrame
	Filter(expr engine.LogicalExpr) DataFrame
	Aggregate(groupBy []engine.LogicalExpr, aggregateExpr []engine.AggregateExpr) DataFrame
	Schema() engine.Schema
	LogicalPlan() engine.LogicalPlan
	Show()
}

type Impl struct {
	plan engine.LogicalPlan
}

func (df *Impl) Show() {
	//TODO implement me
	panic("implement me")
}

func (df *Impl) Project(expr []engine.LogicalExpr) DataFrame {
	return &Impl{engine.Projection{df.plan, expr}}
}

func (df *Impl) Filter(expr engine.LogicalExpr) DataFrame {
	return &Impl{engine.Selection{df.plan, expr}}
}

func (df *Impl) Aggregate(groupBy []engine.LogicalExpr, aggregateExpr []engine.AggregateExpr) DataFrame {
	return &Impl{engine.Aggregate{df.plan, groupBy, aggregateExpr}}
}

func (df *Impl) Schema() engine.Schema {
	return df.plan.Schema()
}

func (df *Impl) LogicalPlan() engine.LogicalPlan {
	return df.plan
}

type ExecutionContext struct{}

func (ec *ExecutionContext) Csv(filename string) DataFrame {
	return &Impl{engine.Scan{filename, &engine.CsvDataSource{Filename: filename}, []string{}}}
}

//func Col(name string) engine.Column {
//	return engine.Column{name}
//}
//func Str(val string) engine.LiteralString {
//	return engine.LiteralString{val}
//}
//func Int(val int64) engine.LiteralInt64 {
//	return engine.LiteralInt64{val}
//}
//func Flt(val float64) engine.LiteralFloat64 {
//	return engine.LiteralFloat64{val}
//}

type Alias struct {
	Expr  engine.LogicalExpr
	Alias string
}

func (expr Alias) ToField(input engine.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: expr.Alias,
		Type: expr.Expr.ToField(input).Type,
	}
}

func (expr Alias) String() string {
	return fmt.Sprintf("%s as %s", expr.Expr.String(), expr.Alias)
}
