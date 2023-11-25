package planlogical

import (
	"context"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	"tiny_planner/pkg/c_sql/b_planner/plancore"
)

func (b *PlanBuilder) buildDelete(ctx context.Context, delete *ast.DeleteStmt) (plancore.Plan, error) {

	panic("implement me")

	//p, err := b.buildResultSetNode(ctx, delete.TableRefs.TableRefs)
	//
	//if delete.Where != nil {
	//	p, err = b.buildSelection(ctx, p, delete.Where)
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	//
	//
	//p, _, err = b.buildProjection(p, delete.Fields.Fields)
	//if err != nil {
	//	return nil, err
	//}
	//
	//proj := LogicalProjection{Expressions: expression.Column2Exprs(p.Schema().Columns)}.Init(b.ctx)
	//proj.SetChildren(p)
	//proj.SetSchema(oldSchema.Clone())
	//proj.names = p.OutputNames()[:oldLen]
	//p = proj
	//
	//del := plancore.Delete{}.Init(b.ctx)
	//
	//return del, err
}
