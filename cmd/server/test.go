package main

import (
	"fmt"
	"tiny_planner/pkg/core/common"
	"tiny_planner/pkg/core/datasource"
	"tiny_planner/pkg/core/execution/context"
)

func main() {

	ctx := context.New()
	testdata := common.ArrowTestData()
	ctx.RegisterCsv("aggregate_test_100", fmt.Sprint(testdata, "/aggregate_test_100.csv"), datasource.CsvReadOptions{})

	df := ctx.Sql("select * from aggregate_test_100")
	df.Show()
}
