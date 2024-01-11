package main

import (
	"tiny_planner/pkg/a_datafusion/core/dataframe"
	"tiny_planner/pkg/a_datafusion/core/datasource"
	"tiny_planner/pkg/a_datafusion/exprLogi"
)

func main() {

	ctx := dataframe.New()
	df := ctx.ReadCsv("test/data/aggregate_test_100.csv",
		datasource.CsvReadOptions{HasHeader: true})

	df = df.
		Filter(exprLogi.Lt(exprLogi.Column{Name: "c1"}, exprLogi.LiteralInt64{N: 10})).
		Project([]exprLogi.LogicalExpr{
			exprLogi.Alias{Expr: exprLogi.Column{Name: "c1"}, Alias: "c1_alias"},
			exprLogi.Alias{Expr: exprLogi.Column{Name: "c2"}, Alias: "c2_alias"},
		})

	df.Show()
}
