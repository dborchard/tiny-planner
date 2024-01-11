package engine

import (
	"fmt"
	"testing"
	exprLogi "tiny_planner/pkg/e_logical_plan"
	datasource "tiny_planner/pkg/h_storage_engine"
)

func TestCsvFile(t *testing.T) {
	ctx := NewContext()
	df := ctx.ReadCsv("/Users/arjunsunilkumar/GolandProjects/0learning/tiny_planner/test/data/aggregate_test_100.csv",
		datasource.CsvReadOptions{HasHeader: true})

	//TODO: ability to pass custom schema

	df = df.
		//Filter(exprLogi.Eq(exprLogi.Column{Name: "state"}, exprLogi.LiteralString{Val: "CO"})).
		Project([]exprLogi.LogicalExpr{
			exprLogi.Column{Name: "c1"},
			exprLogi.Column{Name: "c2"},
		})

	logicalPlan := df.LogicalPlan()
	fmt.Println(exprLogi.PrettyPrint(logicalPlan, 0))

	df.Show()
}

func TestParquetFile(t *testing.T) {
	ctx := NewContext()
	df := ctx.ReadParquet("/Users/arjunsunilkumar/PycharmProjects/mo-benchmark-test/sample_int32.parquet")

	df = df.
		//Filter(exprLogi.Eq(exprLogi.Column{Name: "state"}, exprLogi.LiteralString{Val: "CO"})).
		Project([]exprLogi.LogicalExpr{
			exprLogi.Column{Name: "c1"},
			exprLogi.Column{Name: "c2"},
		})

	logicalPlan := df.LogicalPlan()
	fmt.Println(exprLogi.PrettyPrint(logicalPlan, 0))

	df.Show()
}
