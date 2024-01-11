package dataframe

import (
	"fmt"
	"testing"
	"tiny_planner/pkg/a_datafusion/core/datasource"
	"tiny_planner/pkg/a_datafusion/exprLogi"
)

func TestLogicalPlan_DataFrame(t *testing.T) {
	csv := datasource.CsvDataSource{Filename: "employees.csv", HasHeaders: true, BatchSize: 100}
	scan := exprLogi.Scan{Path: "employee", Source: &csv, Projection: []string{}} // 1. FROM

	df := NewDataFrame(SessionState{}, scan)
	plan := df.
		Filter(exprLogi.Eq(exprLogi.Column{Name: "state"}, exprLogi.LiteralString{Str: "CO"})).
		Project([]exprLogi.LogicalExpr{
			exprLogi.Column{Name: "id"},
			exprLogi.Column{Name: "first_name"},
			exprLogi.Column{Name: "last_name"},
			exprLogi.Column{Name: "state"},
			exprLogi.Column{Name: "salary"},
		}).LogicalPlan()

	actual := exprLogi.PrettyPrint(plan, 0)
	fmt.Println(actual)
}
