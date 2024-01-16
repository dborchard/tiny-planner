package engine

import (
	"fmt"
	"testing"
	logicalplan "tiny_planner/pkg/e_logical_plan"
)

func TestParquetFile(t *testing.T) {
	ctx := NewContext()
	df := ctx.Parquet("../../test/data/c1_c2_int64.parquet")

	df = df.
		Project(
			logicalplan.Column{Name: "c1"},
			logicalplan.Column{Name: "c2"},
		).
		Filter(logicalplan.Eq(
			logicalplan.Column{Name: "c1"},
			logicalplan.LiteralInt64{Val: 100},
		))

	logicalPlan, _ := df.LogicalPlan()
	fmt.Println(logicalplan.PrettyPrint(logicalPlan, 0))

	err := df.Show()
	if err != nil {
		t.Error(err)
	}
}
