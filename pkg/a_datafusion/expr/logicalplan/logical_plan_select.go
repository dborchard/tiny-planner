package logicalplan

import (
	"fmt"
	"tiny_planner/pkg/a_datafusion/expr"
	"tiny_planner/pkg/core/common"
)

type Selection struct {
	Input LogicalPlan
	Expr  expr.Expr
}

func (s Selection) Schema() common.DFSchema {
	return s.Input.Schema()
}

func (s Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.Input}
}

func (s Selection) String() string {
	return fmt.Sprintf("Filter: %s", s.Expr.String())
}
