package logicalplan

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	datasource "tiny_planner/pkg/h_storage_engine"
)

func TestLogicalPlan_LogicalPlan(t *testing.T) {
	var finalPlan LogicalPlan

	{ // build the logical plan

		csv := datasource.CsvDataSource{Filename: "employees.csv", HasHeaders: true, BatchSize: 100}
		scan := Scan{Path: "employee", Source: &csv, Projection: []string{}} // 1. FROM

		filterExpr := Eq(Column{Name: "state"}, LiteralString{Val: "CO"})
		selection := Selection{Next: scan, Filter: filterExpr} // 2. WHERE

		projExpr := []Expr{
			Column{Name: "id"},
			Column{Name: "first_name"},
			Column{Name: "last_name"},
			Column{Name: "state"},
			Column{Name: "salary"},
		}
		projection := Projection{Next: selection, Proj: projExpr} // 3. SELECT col1, col2

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
