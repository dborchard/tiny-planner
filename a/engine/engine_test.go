package engine

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"tiny_planner/pkg/a_datafusion/expr"
	logical_plan2 "tiny_planner/pkg/a_datafusion/expr/logicalplan"
	"tiny_planner/pkg/core/common"
	"tiny_planner/pkg/expr/logical_plan"
)

func TestLogicalPlan(t *testing.T) {
	// data source
	csv := &CsvDataSource{"employees.csv", common.DFSchema{}, true, 100}

	// FROM
	scan := logical_plan2.Scan{"employee", csv, []string{}}

	// WHERE
	filterExpr := expr.Eq(expr.Column{"state"}, expr.LiteralString{"CO"})

	selection := logical_plan2.Selection{scan, filterExpr}

	projection := []logical_plan.LogicalExpr{
		expr.Column{"id"},
		expr.Column{"first_name"},
		expr.Column{"last_name"},
		expr.Column{"state"},
		expr.Column{"salary"},
	}

	plan := logical_plan2.Projection{selection, projection}

	actual := logical_plan2.Format(plan, 0)
	fmt.Println(actual)

	expected := `Projection: #id, #first_name, #last_name, #state, #salary
	Filter: #state = 'CO'
		Scan: employee; projection=None
`
	assert.Equal(t, expected, actual, "plan should equal")

}
