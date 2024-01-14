package engine

import (
	"fmt"
	"testing"
	logicalplan "tiny_planner/pkg/e_logical_plan"
	datasource "tiny_planner/pkg/h_storage_engine"
)

func TestCsvFile(t *testing.T) {
	ctx := NewContext()
	df := ctx.ReadCsv("../../test/data/aggregate_test_100.csv", datasource.CsvReadOptions{HasHeader: true})

	//TODO: ability to pass custom schema

	df = df.
		Project(
			logicalplan.Column{Name: "c1"},
			logicalplan.Column{Name: "c2"},
		).
		Filter(logicalplan.Eq(
			logicalplan.Column{Name: "c1"},
			logicalplan.LiteralString{Val: "c"},
		))

	logicalPlan, _ := df.LogicalPlan()
	fmt.Println(logicalplan.PrettyPrint(logicalPlan, 0))

	_ = df.Show()
}

func TestParquetFile(t *testing.T) {
	ctx := NewContext()
	df := ctx.ReadParquet("../../test/data/c1_c2_int64.parquet")

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

	_ = df.Show()
}
