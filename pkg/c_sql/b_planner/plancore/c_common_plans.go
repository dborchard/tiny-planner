package plancore

import (
	"context"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	"tiny_planner/pkg/b_catalog"
)

type Insert struct {
	BasePlan

	TableSchema *catalog.TableDef
	Columns     []ExprCol
}

func (p *Insert) Init(ctx context.Context) *Insert {
	p.BasePlan = NewBasePlan(ctx)
	return p
}

type Delete struct {
	BasePlan
}

// DDL represents a DDL statement plan.
type DDL struct {
	BasePlan
	Statement ast.DDLNode
}
