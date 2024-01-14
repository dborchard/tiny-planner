package logicalplan

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	datasource "tiny_planner/pkg/h_storage_engine"
)

func TestLogicalPlan_Builder(t *testing.T) {

	csv := datasource.CsvDataSource{Filename: "employees.csv", HasHeaders: true, BatchSize: 100}

	plan, _ := NewBuilder().
		Scan("employees.csv", &csv, []string{}).
		Filter(Eq(Column{Name: "state"}, LiteralString{Val: "CO"})).
		Project(
			Column{Name: "id"},
			Column{Name: "first_name"},
			Column{Name: "last_name"},
			Column{Name: "state"},
			Column{Name: "salary"},
		).
		Build()

	// Print the logical plan and Test
	actual := PrettyPrint(plan, 0)
	fmt.Println(actual)

	expected := `Projection: #id, #first_name, #last_name, #state, #salary
	Filter: #state = 'CO'
		Scan: employees.csv; projExpr=None
`
	assert.Equal(t, expected, actual, "projection should equal")
}
