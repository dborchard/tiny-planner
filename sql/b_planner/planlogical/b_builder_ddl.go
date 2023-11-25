package planlogical

import (
	"context"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	plancore2 "tiny_planner/sql/b_planner/plancore"
)

func (b *PlanBuilder) buildDDL(ctx context.Context, node ast.DDLNode) (plancore2.Plan, error) {
	p := &plancore2.DDL{Statement: node}
	return p, nil
}
