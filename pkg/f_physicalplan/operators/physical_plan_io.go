package operators

import (
	"context"
	"strings"
	execution "tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

//----------------- Scan -----------------

type Scan struct {
	Source     datasource.TableReader
	Projection []string
	next       PhysicalPlan
}

func (s *Scan) SetNext(next PhysicalPlan) {
	s.next = next
}

func (s *Scan) Callback(ctx context.Context, r containers.IBatch) error {
	panic("bug")
}

func (s *Scan) Schema() containers.ISchema {
	if len(s.Projection) == 0 {
		return s.Source.Schema()
	}
	schema := s.Source.Schema()
	return schema.Select(s.Projection)
}

func (s *Scan) Execute(ctx execution.TaskContext, _ datasource.Callback) error {

	childrenCallbacks := make([]datasource.Callback, 0, len(s.Children()))
	for _, plan := range s.Children() {
		childrenCallbacks = append(childrenCallbacks, plan.Callback)
	}

	options := []datasource.Option{
		datasource.WithProjection(s.Projection...),
	}
	return s.Source.Iterator(ctx, childrenCallbacks, options...)
}

func (s *Scan) Children() []PhysicalPlan {
	return []PhysicalPlan{s.next}
}

func (s *Scan) String() string {
	schema := s.Schema()
	return "Scan: schema=" + schema.String() + ", projection=" + strings.Join(s.Projection, ",")
}

// --------Out---------

type Out struct {
	OutputCallback datasource.Callback
}

func (e Out) Schema() containers.ISchema {
	panic("bug")
}

func (e Out) Children() []PhysicalPlan {
	panic("bug")
}

func (e Out) Callback(ctx context.Context, r containers.IBatch) error {
	return e.OutputCallback(ctx, r)
}

func (e Out) Execute(ctx execution.TaskContext, callback datasource.Callback) error {
	panic("bug")
}

func (e Out) SetNext(next PhysicalPlan) {
	panic("bug")
}
