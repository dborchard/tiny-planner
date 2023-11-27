package planphysical

import (
	"context"
	"tiny_planner/pkg/c_sql/b_planner/plancore"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
	expression "tiny_planner/pkg/c_sql/c_exec_engine/c_expression_eval"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type PhysicalPlan interface {
	plancore.Plan

	// ToPB converts the physical plan to a protobuf message.
	ToPB(ctx context.Context) (any, error)
}

type basePhysicalPlan struct {
	plancore.BasePlan

	Process *process.Process
	fill    func(any, *batch.Batch) error
}

func (p *basePhysicalPlan) ToPB(ctx context.Context) (any, error) {
	return nil, nil
}

var _ PhysicalPlan = &basePhysicalPlan{}
var _ PhysicalPlan = &PhysicalSelection{}
var _ PhysicalPlan = &PhysicalProjection{}
var _ PhysicalPlan = &PhysicalTableReader{}

type PhysicalSelection struct {
	basePhysicalPlan
}

type PhysicalProjection struct {
	basePhysicalPlan
	Expressions []expression.Expr
}

type PhysicalTableReader struct {
	basePhysicalPlan
}
