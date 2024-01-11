package exprPhy

import (
	"fmt"
	"strings"
	execution "tiny_planner/pkg/h_exec_runtime"
	datasource "tiny_planner/pkg/i_datasource"
	containers "tiny_planner/pkg/j_containers"
)

type ExecutionPlan interface {
	Schema() containers.Schema
	Children() []ExecutionPlan
	Execute(ctx execution.TaskContext) []containers.Batch
}

var _ ExecutionPlan = ScanExec{}
var _ ExecutionPlan = ProjectionExec{}
var _ ExecutionPlan = SelectionExec{}

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

func (s ScanExec) Execute(ctx execution.TaskContext) []containers.Batch {
	return s.Source.Scan(s.Projection, ctx)
}

func (s ScanExec) Children() []ExecutionPlan {
	return []ExecutionPlan{}
}

func (s ScanExec) String() string {
	return "ScanExec: Sch=" + s.Schema().String() + ", projection=" + strings.Join(s.Projection, ",")
}

//----------------- ProjectionExec -----------------

type ProjectionExec struct {
	Input ExecutionPlan
	Sch   containers.Schema
	Proj  []Expression
}

func (p ProjectionExec) String() string {
	return fmt.Sprintf("ProjectionExec: %s", p.Proj)
}

func (p ProjectionExec) Schema() containers.Schema {
	return p.Sch
}

func (p ProjectionExec) Execute(ctx execution.TaskContext) []containers.Batch {
	input := p.Input.Execute(ctx)
	output := make([]containers.Batch, len(input))

	for i, batch := range input {
		vectors := make([]containers.IVector, len(p.Proj))
		for j, expr := range p.Proj {
			vectors[j] = expr.Evaluate(batch)
		}
		output[i] = containers.Batch{Schema: p.Sch, Fields: vectors}
	}
	return output
}

func (p ProjectionExec) Children() []ExecutionPlan {
	return []ExecutionPlan{p.Input}
}

//----------------- SelectionExec -----------------

type SelectionExec struct {
	Input  ExecutionPlan
	Filter Expression
}

func (s SelectionExec) Schema() containers.Schema {
	return s.Input.Schema()
}

func (s SelectionExec) Children() []ExecutionPlan {
	return []ExecutionPlan{s.Input}
}

func (s SelectionExec) Execute(ctx execution.TaskContext) []containers.Batch {
	input := s.Input.Execute(ctx)
	output := make([]containers.Batch, len(input))
	for i, batch := range input {
		result := s.Filter.Evaluate(batch)
		schema := batch.Schema
		columnCount := len(schema.Fields())
		filtered := make([]containers.IVector, len(batch.Fields))
		for j := 0; j < columnCount; j++ {
			filtered[j] = filter(batch.Fields[j], result)
		}
		output[i] = containers.Batch{Schema: batch.Schema, Fields: filtered}
	}
	return output
}

func filter(vector containers.IVector, selection containers.IVector) containers.IVector {
	var filteredVector []any
	for i := 0; i < selection.Len(); i++ {
		if selection.GetValue(i).(bool) {
			filteredVector = append(filteredVector, vector.GetValue(i))
		}
	}
	return containers.NewVector(vector.DataType(), len(filteredVector), filteredVector)
}
