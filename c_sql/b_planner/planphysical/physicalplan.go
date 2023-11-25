package planphysical

import (
	"context"
	"tiny_planner/c_sql/b_planner/plancore"
	execution "tiny_planner/c_sql/c_execution"
)

type PhysicalPlan interface {
	plancore.Plan

	// ToPB converts the physical plan to a protobuf message.
	ToPB(ctx context.Context) (execution.Executor, error)
}

type basePhysicalPlan struct {
	plancore.BasePlan
}

func (p *basePhysicalPlan) ToPB(ctx context.Context) (execution.Executor, error) {
	executorBuilder := execution.NewExecutorBuilder(ctx, p.Schema())
	return executorBuilder.Build(p)
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
}

type PhysicalTableReader struct {
	basePhysicalPlan
}
