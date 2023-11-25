package planlogical

import (
	"context"
	"errors"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	"tiny_planner/b_catalog"
	"tiny_planner/sql/b_planner/plancore"
)

type PlanBuilder struct {
	ctx context.Context
	is  *catalog.TableDef
}

func NewPlanBuilder(sctx context.Context, is *catalog.TableDef) *PlanBuilder {
	return &PlanBuilder{
		ctx: sctx,
		is:  is,
	}
}

func (b *PlanBuilder) Build(ctx context.Context, node ast.Node) (plancore.Plan, error) {
	switch x := node.(type) {
	case *ast.InsertStmt:
		return b.buildInsert(ctx, x)
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.DeleteStmt:
		return b.buildDelete(ctx, x)
	case ast.DDLNode:
		return b.buildDDL(ctx, x)
	}
	return nil, errors.New("not implemented yet")
}
