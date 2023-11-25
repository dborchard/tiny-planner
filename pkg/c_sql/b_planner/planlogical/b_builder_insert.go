package planlogical

import (
	"context"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	"tiny_planner/pkg/c_sql/b_planner/plancore"
)

func (b *PlanBuilder) buildInsert(ctx context.Context, insert *ast.InsertStmt) (plancore.Plan, error) {
	panic("implement me")

	//ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	//if !ok {
	//	return nil, errors.New("table does not exist")
	//}
	//
	//tn, ok := ts.Source.(*ast.TableName)
	//if !ok {
	//	return nil, errors.New("table does not exist")
	//}
	//
	//tableInfo := tn.TableInfo
	//
	//// Build Schema with DBName otherwise ColumnRef with DBName cannot match any Column in Schema.
	//tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	//if !ok {
	//	return nil, errors.New(fmt.Sprintf("Can't get table %s.", tableInfo.Name.O))
	//}
	//
	//_insertPlan := plancore.Insert{
	//	Table:       tableInPlan,
	//	Columns:     insert.Columns,
	//	TableSchema: schema,
	//}
	//insertPlan := _insertPlan.Init(b.ctx)
	//
	//return insertPlan, nil
}
