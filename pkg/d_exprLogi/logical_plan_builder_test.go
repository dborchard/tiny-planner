package exprLogi

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	datasource "tiny_planner/pkg/c_datasource"
)

func TestLogicalPlan_Builder(t *testing.T) {

	csv := datasource.CsvDataSource{Filename: "employees.csv", HasHeaders: true, BatchSize: 100}
	scan := Scan{Path: "employee", Source: &csv, Projection: []string{}} // 1. FROM

	plan := From(scan).
		Filter(Eq(Column{Name: "state"}, LiteralString{Val: "CO"})).
		Project([]LogicalExpr{
			Column{Name: "id"},
			Column{Name: "first_name"},
			Column{Name: "last_name"},
			Column{Name: "state"},
			Column{Name: "salary"},
		}).
		Build()

	// Print the logical plan and Test
	actual := PrettyPrint(plan, 0)
	fmt.Println(actual)

	expected := `Projection: #id, #first_name, #last_name, #state, #salary
	Filter: #state = 'CO'
		Scan: employee; projExpr=None
`
	assert.Equal(t, expected, actual, "projection should equal")
}
