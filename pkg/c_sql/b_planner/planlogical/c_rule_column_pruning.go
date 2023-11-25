package planlogical

import (
	"context"
	"tiny_planner/pkg/b_catalog"
	"tiny_planner/pkg/c_sql/b_planner/plancore"
)

type columnPruner struct {
}

func (*columnPruner) Name() string {
	return "column_prune"
}

func (*columnPruner) Optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	err := lp.PruneColumns(plancore.ColDefToExprCol(lp.Schema().ColDefs))
	return lp, err
}

func (p *baseLogicalPlan) PruneColumns(cols []plancore.ExprCol) error {
	panic("unimplemented")
}

func (p *LogicalProjection) PruneColumns(parentUsedCols []plancore.ExprCol) error {
	child := p.children[0]

	used := getUsedList(parentUsedCols, p.Schema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.Expressions = append(p.Expressions[:i], p.Expressions[i+1:]...)
		}
	}

	if child == nil {
		return nil
	}
	return child.PruneColumns(parentUsedCols)
}

func getUsedList(usedCols []plancore.ExprCol, schema *catalog.TableDef) []bool {
	used := make([]bool, len(schema.ColDefs))
	for i, col := range schema.ColDefs {
		used[i] = false
		for _, usedCol := range usedCols {
			if col.Idx == usedCol.ColIdx {
				used[i] = true
				break
			}
		}
	}
	return used
}

func (p *LogicalSelection) PruneColumns(parentUsedCols []plancore.ExprCol) error {
	child := p.children[0]
	if child == nil {
		return nil
	}
	parentUsedCols = ExtractColumnsFromExpressions(parentUsedCols, p.Conditions)
	return child.PruneColumns(parentUsedCols)
}

func ExtractColumnsFromExpressions(result []plancore.ExprCol, exprs []plancore.Expr) []plancore.ExprCol {
	for _, expr := range exprs {
		result = extractColumns(result, expr)
	}
	return result
}

func extractColumns(result []plancore.ExprCol, expr plancore.Expr) []plancore.ExprCol {
	switch v := expr.(type) {
	case *plancore.ExprCol:
		result = append(result, *v)
	case *plancore.ExprFunc:
		for _, arg := range v.Args {
			result = extractColumns(result, arg)
		}
	}
	return result
}

func (p *DataSource) PruneColumns(parentUsedCols []plancore.ExprCol) error {
	used := getUsedList(parentUsedCols, p.Schema())

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.Columns = append(p.Columns[:i], p.Columns[i+1:]...)
		}
	}

	return nil
}
