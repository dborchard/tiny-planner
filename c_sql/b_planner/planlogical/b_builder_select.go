package planlogical

import (
	"context"
	"errors"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	"github.com/blastrain/vitess-sqlparser/tidbparser/dependency/model"
	"github.com/blastrain/vitess-sqlparser/tidbparser/parser/opcode"
	"tiny_planner/a_containers/a_types"
	catalog "tiny_planner/b_catalog"
	"tiny_planner/c_sql/b_planner/plancore"
)

func (b *PlanBuilder) buildSelect(ctx context.Context, sel *ast.SelectStmt) (p LogicalPlan, err error) {
	if sel.From != nil {
		p, err = b.buildResultSetNode(ctx, sel.From.TableRefs)
		if err != nil {
			return nil, err
		}
	}

	if sel.Where != nil {
		p, err = b.buildSelection(ctx, p, sel.Where)
		if err != nil {
			return nil, err
		}
	}

	p, _, err = b.buildProjection(p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (b *PlanBuilder) buildResultSetNode(ctx context.Context, node ast.ResultSetNode) (p LogicalPlan, err error) {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(ctx, x)
	case *ast.TableSource:
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p, err = b.buildSelect(ctx, v)
		case *ast.TableName:
			p, err = b.buildDataSource(ctx, v, &x.AsName)
		default:
			return nil, errors.New("unsupported table source")
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	default:
		return nil, errors.New("unsupported table source")
	}
}

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left)
	}
	panic("not implemented yet")
}

func (b *PlanBuilder) buildDataSource(ctx context.Context, tn *ast.TableName, asName *model.CIStr) (LogicalPlan, error) {
	dbName := tn.Schema
	tbl, err := b.is.TableByName(dbName, tn.Name)
	if err != nil {
		return nil, err
	}

	_ds := DataSource{
		DBName:  dbName,
		table:   tbl,
		Columns: make([]plancore.ExprCol, 0),
	}
	ds := _ds.Init(b.ctx)

	schema := catalog.NewTableDef("", make([]*catalog.ColDef, 0))
	for i, col := range tbl.ColDefs {
		ds.Columns = append(_ds.Columns, plancore.ExprCol{
			Type:   col.Type,
			ColIdx: i,
		})
		_ = schema.AppendCol(col.Name, col.Type)
	}
	ds.SetSchema(schema)

	return ds, nil
}

func (b *PlanBuilder) buildSelection(ctx context.Context, p LogicalPlan, where ast.ExprNode) (LogicalPlan, error) {
	_selection := LogicalSelection{}
	selection := _selection.Init(b.ctx)
	selection.Conditions = make([]plancore.Expr, 0)

	conditions := splitWhere(where)
	for _, _ = range conditions {
		//TODO: fix later
		selection.Conditions = append(selection.Conditions, nil)
	}

	selection.SetChildren(p)
	return selection, nil
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func (b *PlanBuilder) buildProjection(p LogicalPlan, fields []*ast.SelectField) (LogicalPlan, int, error) {
	_proj := LogicalProjection{}
	proj := _proj.Init(b.ctx)
	proj.Expressions = make([]plancore.Expr, 0, len(fields))
	schema := catalog.NewTableDef("", make([]*catalog.ColDef, 0))

	for _, field := range fields {
		switch v := field.Expr.(type) {
		case *ast.ColumnNameExpr:
			//colName := v.Name.Name.L
			proj.Expressions = append(proj.Expressions, &plancore.ExprCol{
				Type:   types.T_int32.ToType(),
				ColIdx: 0,
			})
			_ = schema.AppendCol(v.Name.Name.L, types.T_int32.ToType())
		case *ast.FuncCallExpr:
			proj.Expressions = append(proj.Expressions, &plancore.ExprFunc{
				Name: v.FnName.L,
				Args: plancore.ArgsToExprs(v.Args),
			})
			schema.ColDefs = append(schema.ColDefs, plancore.ArgsToColDefs(v.Args)...)
		}

	}

	proj.SetSchema(schema)
	proj.SetChildren(p)
	return proj, 0, nil
}
