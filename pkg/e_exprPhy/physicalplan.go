package exprPhy

import (
	"fmt"
	"strings"
	containers "tiny_planner/pkg/a_containers"
	datasource "tiny_planner/pkg/c_datasource"
)

type PhysicalPlan interface {
	Schema() containers.Schema
	Children() []PhysicalPlan
	Execute() []containers.Batch
}

var _ PhysicalPlan = ScanExec{}
var _ PhysicalPlan = ProjectionExec{}
var _ PhysicalPlan = SelectionExec{}

//----------------- ScanExec -----------------

type ScanExec struct {
	Source     datasource.DataSource
	Projection []string
}

func (s ScanExec) Schema() containers.Schema {
	if len(s.Projection) == 0 {
		return s.Source.Schema()
	}
	return s.Source.Schema().Select(s.Projection)
}

func (s ScanExec) Execute() []containers.Batch {
	return s.Source.Scan(s.Projection)
}

func (s ScanExec) Children() []PhysicalPlan {
	return []PhysicalPlan{}
}

func (s ScanExec) String() string {
	return "ScanExec: Sch=" + s.Schema().String() + ", projection=" + strings.Join(s.Projection, ",")
}

//----------------- ProjectionExec -----------------

type ProjectionExec struct {
	Input PhysicalPlan
	Sch   containers.Schema
	Proj  []Expression
}

func (p ProjectionExec) String() string {
	return fmt.Sprintf("ProjectionExec: %s", p.Proj)
}

func (p ProjectionExec) Schema() containers.Schema {
	return p.Sch
}

func (p ProjectionExec) Execute() []containers.Batch {
	input := p.Input.Execute()
	output := make([]containers.Batch, len(input))

	for i, batch := range input {
		vectors := make([]containers.Vector, len(p.Proj))
		for j, expr := range p.Proj {
			vectors[j] = expr.Evaluate(batch)
		}
		output[i] = containers.Batch{Schema: p.Sch, Fields: vectors}
	}
	return output
}

func (p ProjectionExec) Children() []PhysicalPlan {
	return []PhysicalPlan{p.Input}
}

//----------------- SelectionExec -----------------

type SelectionExec struct {
	Input  PhysicalPlan
	Filter Expression
}

func (s SelectionExec) Schema() containers.Schema {
	return s.Input.Schema()
}

func (s SelectionExec) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Input}
}

func (s SelectionExec) Execute() []containers.Batch {
	input := s.Input.Execute()
	output := make([]containers.Batch, len(input))
	for i, batch := range input {
		result := s.Filter.Evaluate(batch)
		schema := batch.Schema
		columnCount := len(schema.Fields())
		filtered := make([]containers.Vector, len(batch.Fields))
		for j := 0; j < columnCount; j++ {
			filtered[j] = filter(batch.Fields[j], result)
		}
		output[i] = containers.Batch{Schema: batch.Schema, Fields: filtered}
	}
	return output
}

func filter(vector containers.Vector, selection containers.Vector) containers.Vector {
	var filteredVector []any
	for i := 0; i < selection.Len(); i++ {
		if selection.GetValue(i).(bool) {
			filteredVector = append(filteredVector, vector.GetValue(i))
		}
	}
	return containers.NewArray(vector.DataType(), len(filteredVector), filteredVector)
}
