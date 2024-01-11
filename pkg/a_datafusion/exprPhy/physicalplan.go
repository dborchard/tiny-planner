package exprPhy

import (
	"fmt"
	"strings"
	"tiny_planner/pkg/a_datafusion/common"
	"tiny_planner/pkg/a_datafusion/core/datasource"
)

type PhysicalPlan interface {
	Schema() common.Schema
	Children() []PhysicalPlan
	Execute() []common.Batch
}

var _ PhysicalPlan = ScanExec{}
var _ PhysicalPlan = ProjectionExec{}
var _ PhysicalPlan = SelectionExec{}

//----------------- ScanExec -----------------

type ScanExec struct {
	DataSource datasource.DataSource
	Projection []string
}

func (s ScanExec) Schema() common.Schema {
	return s.DataSource.Schema().Select(s.Projection)
}

func (s ScanExec) Execute() []common.Batch {
	return s.DataSource.Scan(s.Projection)
}

func (s ScanExec) Children() []PhysicalPlan {
	return []PhysicalPlan{}
}

func (s ScanExec) String() string {
	return "ScanExec: common.Sch=" + s.Schema().String() + ", projection=" + strings.Join(s.Projection, ",")
}

//----------------- ProjectionExec -----------------

type ProjectionExec struct {
	Input PhysicalPlan
	Sch   common.Schema
	Proj  []Expression
}

func (p ProjectionExec) String() string {
	return fmt.Sprintf("ProjectionExec: %s", p.Proj)
}

func (p ProjectionExec) Schema() common.Schema {
	return p.Sch
}

func (p ProjectionExec) Execute() []common.Batch {
	input := p.Input.Execute()
	output := make([]common.Batch, len(input))

	for i, batch := range input {
		vectors := make([]common.Vector, len(p.Proj))
		for j, expr := range p.Proj {
			vectors[j] = expr.Evaluate(batch)
		}
		output[i] = common.Batch{Schema: p.Sch, Fields: vectors}
	}
	return output
}

func (p ProjectionExec) Children() []PhysicalPlan {
	return []PhysicalPlan{p.Input}
}

//----------------- SelectionExec -----------------

type SelectionExec struct {
	Input PhysicalPlan
	Expr  Expression
}

func (s SelectionExec) Schema() common.Schema {
	return s.Input.Schema()
}

func (s SelectionExec) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Input}
}

func (s SelectionExec) Execute() []common.Batch {
	input := s.Input.Execute()
	output := make([]common.Batch, len(input))
	for i, batch := range input {
		result := s.Expr.Evaluate(batch)
		schema := batch.Schema
		columnCount := len(schema.Fields())
		filtered := make([]common.Vector, len(batch.Fields))
		for j := 0; j < columnCount; j++ {
			filtered[j] = filter(batch.Fields[j], result)
		}
		output[i] = common.Batch{Schema: batch.Schema, Fields: filtered}
	}
	return output
}

func filter(vector common.Vector, selection common.Vector) common.Vector {
	var filteredVector []any
	for i := 0; i < selection.Len(); i++ {
		if selection.GetValue(i).(bool) {
			filteredVector = append(filteredVector, vector.GetValue(i))
		}
	}
	return common.NewArray(vector.DataType(), len(filteredVector), filteredVector)
}
