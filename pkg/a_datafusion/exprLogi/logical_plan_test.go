package exprLogi

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"tiny_planner/pkg/a_datafusion/core/datasource"
)

func TestLogicalPlan_LogicalPlan(t *testing.T) {
	var finalPlan LogicalPlan

	{ // build the logical plan

		csv := datasource.CsvDataSource{Filename: "employees.csv", HasHeaders: true, BatchSize: 100}
		scan := Scan{Path: "employee", Source: &csv, Projection: []string{}} // 1. FROM

		filterExpr := Eq(Column{Name: "state"}, LiteralString{Str: "CO"})
		selection := Selection{Input: scan, Expr: filterExpr} // 2. WHERE

		projExpr := []LogicalExpr{
			Column{Name: "id"},
			Column{Name: "first_name"},
			Column{Name: "last_name"},
			Column{Name: "state"},
			Column{Name: "salary"},
		}
		projection := Projection{Input: selection, Expr: projExpr} // 3. SELECT col1, col2

		finalPlan = projection
	}

	// Print the logical plan and Test
	actual := PrettyPrint(finalPlan, 0)
	fmt.Println(actual)

	expected := `Projection: #id, #first_name, #last_name, #state, #salary
	Filter: #state = 'CO'
		Scan: employee; projExpr=None
`
	assert.Equal(t, expected, actual, "projection should equal")

}
