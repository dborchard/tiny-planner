package physicalplan

import (
	"fmt"
	"strings"
	execution "tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

type PhysicalPlan interface {
	Schema() (containers.ISchema, error)
	Children() []PhysicalPlan
	Execute(ctx execution.TaskContext) ([]containers.IBatch, error)
}

var _ PhysicalPlan = Scan{}
var _ PhysicalPlan = Projection{}
var _ PhysicalPlan = Selection{}

//----------------- Scan -----------------

type Scan struct {
	Source     datasource.TableReader
	Projection []string
}

func (s Scan) Schema() (containers.ISchema, error) {
	if len(s.Projection) == 0 {
		return s.Source.Schema()
	}
	schema, err := s.Source.Schema()
	if err != nil {
		return nil, err
	}
	return schema.Select(s.Projection)
}

func (s Scan) Execute(ctx execution.TaskContext) ([]containers.IBatch, error) {
	return s.Source.Iterator(s.Projection, ctx)
}

func (s Scan) Children() []PhysicalPlan {
	return []PhysicalPlan{}
}

func (s Scan) String() string {
	schema, err := s.Schema()
	if err != nil {
		panic(err)
	}
	return "Scan: Sch=" + schema.String() + ", projection=" + strings.Join(s.Projection, ",")
}

//----------------- Projection -----------------

type Projection struct {
	Input PhysicalPlan
	Sch   containers.ISchema
	Proj  []Expr
}

func (p Projection) String() string {
	return fmt.Sprintf("Projection: %s", p.Proj)
}

func (p Projection) Schema() (containers.ISchema, error) {
	return p.Sch, nil
}

func (p Projection) Execute(ctx execution.TaskContext) ([]containers.IBatch, error) {
	input, err := p.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}
	output := make([]containers.IBatch, len(input))

	for i, batch := range input {
		vectors := make([]containers.IVector, len(p.Proj))
		for j, expr := range p.Proj {
			vectors[j], err = expr.Evaluate(batch)
			if err != nil {
				return nil, err
			}
		}
		output[i] = containers.NewBatch(p.Sch, vectors)
	}
	return output, nil
}

func (p Projection) Children() []PhysicalPlan {
	return []PhysicalPlan{p.Input}
}

//----------------- Selection -----------------

type Selection struct {
	Input  PhysicalPlan
	Filter Expr
}

func (s Selection) Schema() (containers.ISchema, error) {
	return s.Input.Schema()
}

func (s Selection) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Input}
}

func (s Selection) Execute(ctx execution.TaskContext) ([]containers.IBatch, error) {
	input, err := s.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}
	for i, batch := range input {
		sel, err := s.Filter.Evaluate(batch)
		if err != nil {
			return nil, err
		}
		input[i].Shrink(sel)
	}
	return input, nil
}
