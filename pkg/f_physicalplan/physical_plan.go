package physicalplan

import (
	"context"
	"fmt"
	"strings"
	execution "tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

type PhysicalPlan interface {
	Schema() containers.ISchema
	Children() []PhysicalPlan
	Callback(ctx context.Context, r containers.IBatch) error
	Execute(ctx execution.TaskContext, callback datasource.Callback) error
	SetNext(next PhysicalPlan)
}

var _ PhysicalPlan = &Scan{}
var _ PhysicalPlan = &Projection{}
var _ PhysicalPlan = &Selection{}
var _ PhysicalPlan = &Out{}

//----------------- Scan -----------------

type Scan struct {
	callback   datasource.Callback
	Source     datasource.TableReader
	Projection []string
	Next       PhysicalPlan
}

func (s *Scan) SetNext(next PhysicalPlan) {
	s.Next = next
}

func (s *Scan) Callback(ctx context.Context, r containers.IBatch) error {
	return s.callback(ctx, r)
}

func (s *Scan) Schema() containers.ISchema {
	if len(s.Projection) == 0 {
		return s.Source.Schema()
	}
	schema := s.Source.Schema()
	return schema.Select(s.Projection)
}

func (s *Scan) Execute(ctx execution.TaskContext, callback datasource.Callback) error {
	//s.callback = callback

	callbacks := make([]datasource.Callback, 0, len(s.Children()))
	for _, plan := range s.Children() {
		callbacks = append(callbacks, plan.Callback)
	}

	return s.Source.Iterator(s.Projection, ctx, callbacks)
}

func (s *Scan) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Next}
}

func (s *Scan) String() string {
	schema := s.Schema()
	return "Scan: schema=" + schema.String() + ", projection=" + strings.Join(s.Projection, ",")
}

//----------------- Projection -----------------

type Projection struct {
	Next PhysicalPlan
	Sch  containers.ISchema
	Proj []Expr
}

func (p *Projection) SetNext(next PhysicalPlan) {
	p.Next = next
}

func (p *Projection) Callback(ctx context.Context, batch containers.IBatch) error {
	vectors := make([]containers.IVector, len(p.Proj))
	var err error
	for colIdx, expr := range p.Proj {
		vectors[colIdx], err = expr.Evaluate(batch)
		if err != nil {
			return err
		}
	}
	return p.Next.Callback(ctx, containers.NewBatch(p.Sch, vectors))
}

func (p *Projection) String() string {
	return fmt.Sprintf("Projection: %s", p.Proj)
}

func (p *Projection) Schema() containers.ISchema {
	return p.Sch
}

func (p *Projection) Execute(ctx execution.TaskContext, callback datasource.Callback) error {
	panic("error in Projection Execute")
}

func (p *Projection) Children() []PhysicalPlan {
	return []PhysicalPlan{p.Next}
}

//----------------- Selection -----------------

type Selection struct {
	Next   PhysicalPlan
	Filter Expr
}

func (s *Selection) SetNext(next PhysicalPlan) {
	s.Next = next
}

func (s *Selection) Callback(ctx context.Context, batch containers.IBatch) error {
	sel, err := s.Filter.Evaluate(batch)
	if err != nil {
		return err
	}
	batch.Shrink(sel)
	return s.Next.Callback(ctx, batch)
}

func (s *Selection) Schema() containers.ISchema {
	return s.Next.Schema()
}

func (s *Selection) Children() []PhysicalPlan {
	return []PhysicalPlan{s.Next}
}

func (s *Selection) Execute(ctx execution.TaskContext, callback datasource.Callback) error {
	panic("error in Selection Execute")
}

// --------

type Out struct {
	CallbackPtr datasource.Callback
	Scan        PhysicalPlan
}

func (e Out) Schema() containers.ISchema {
	return nil
}

func (e Out) Children() []PhysicalPlan {
	return nil
}

func (e Out) Callback(ctx context.Context, r containers.IBatch) error {
	return e.CallbackPtr(ctx, r)
}

func (e Out) Execute(ctx execution.TaskContext, callback datasource.Callback) error {
	e.CallbackPtr = callback
	return e.Scan.Execute(ctx, e.CallbackPtr)
}

func (e Out) SetNext(next PhysicalPlan) {
	panic("bug")
}
