package expr

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	logical_plan2 "tiny_planner/pkg/a_datafusion/expr/logicalplan"
)

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
	Expr  Expr
	Alias string
}

func (expr Alias) ToField(input logical_plan2.LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: expr.Alias,
		Type: expr.Expr.ToField(input).Type,
	}
}

func (expr Alias) String() string {
	return fmt.Sprintf("%s as %s", expr.Expr.String(), expr.Alias)
}
