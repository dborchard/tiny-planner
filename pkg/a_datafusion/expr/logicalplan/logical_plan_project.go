package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strings"
	"tiny_planner/pkg/a_datafusion/expr"
	"tiny_planner/pkg/core/common"
)

type Projection struct {
	Input LogicalPlan
	Expr  []expr.Expr
}

func (p Projection) Schema() common.DFSchema {
	var fields []arrow.Field
	for _, e := range p.Expr {
		fields = append(fields, e.ToField(p.Input))
	}
	return common.DFSchema{Schema: arrow.NewSchema(fields, nil)}
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Input}
}

func (p Projection) String() string {
	var strs []string
	for _, e := range p.Expr {
		strs = append(strs, e.String())
	}
	s := strings.Join(strs, ", ")
	return fmt.Sprintf("Projection: %s", s)
}
