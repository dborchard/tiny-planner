package operators

import (
	"context"
	"strings"
	execution "tiny_planner/pkg/e_exec_runtime"
	datasource "tiny_planner/pkg/f_storage_engine"
	containers "tiny_planner/pkg/g_containers"
)

//----------------- Input -----------------

type Input struct {
	Source datasource.TableReader

	//TODO: make this Expr instead of string
	// Add more things like Distinct or Filter etc.
	Projection []string
	next       PhysicalPlan
}

func (s *Input) SetNext(next PhysicalPlan) {
	s.next = next
}

func (s *Input) Callback(ctx context.Context, r containers.IBatch) error {
	panic("bug")
}

func (s *Input) Schema() containers.ISchema {
	if len(s.Projection) == 0 {
		return s.Source.Schema()
	}
	schema := s.Source.Schema()
	return schema.Select(s.Projection)
}

func (s *Input) Execute(ctx execution.TaskContext, _ datasource.Callback) error {

	childrenCallbacks := make([]datasource.Callback, 0, len(s.Children()))
	for _, plan := range s.Children() {
		childrenCallbacks = append(childrenCallbacks, plan.Callback)
	}

	options := []datasource.Option{
		datasource.WithProjection(s.Projection...),
	}
	return s.Source.Iterator(ctx, childrenCallbacks, options...)
}

func (s *Input) Children() []PhysicalPlan {
	return []PhysicalPlan{s.next}
}

func (s *Input) String() string {
	schema := s.Schema()
	return "Input: schema=" + schema.String() + ", projection=" + strings.Join(s.Projection, ",")
}

// --------Output---------

type Output struct {
	OutputCallback datasource.Callback
}

func (e Output) Schema() containers.ISchema {
	panic("bug")
}

func (e Output) Children() []PhysicalPlan {
	panic("bug")
}

func (e Output) Callback(ctx context.Context, r containers.IBatch) error {
	return e.OutputCallback(ctx, r)
}

func (e Output) Execute(ctx execution.TaskContext, callback datasource.Callback) error {
	panic("bug")
}

func (e Output) SetNext(next PhysicalPlan) {
	panic("bug")
}
