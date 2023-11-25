package planlogical

import (
	"context"
	"github.com/blastrain/vitess-sqlparser/tidbparser/parser"
	"github.com/stretchr/testify/assert"
	"testing"
	catalog "tiny_planner/b_catalog"
)

func TestPruneColumns(t *testing.T) {
	sql := "select mock_0 from (select mock_0,mock_1 from t1) as t2;"
	ctx := context.TODO()
	parsr := parser.New()
	stmt, err := parsr.ParseOneStmt(sql, "", "")
	assert.Nil(t, err)

	builder := NewPlanBuilder(context.TODO(), catalog.MockTableDef("t1", 2))
	p, err := builder.Build(ctx, stmt)
	assert.Nil(t, err)

	assert.Equal(t, "Projection: Column#0(col INT32), Projection: Column#0(col INT32), Column#1(col INT32), DataSource: table:t1", p.ExplainInfo())

	p, err = Optimize(ctx, p.(LogicalPlan))
	assert.Equal(t, "Projection: Column#0(col INT32), Projection: Column#0(col INT32), DataSource: table:t1", p.ExplainInfo())
}
