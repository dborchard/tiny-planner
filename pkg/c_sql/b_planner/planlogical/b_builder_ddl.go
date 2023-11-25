package planlogical

import (
	"context"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	"tiny_planner/pkg/c_sql/b_planner/plancore"
)

func (b *PlanBuilder) buildDDL(ctx context.Context, node ast.DDLNode) (plancore.Plan, error) {
	p := &plancore.DDL{Statement: node}
	return p, nil
}
