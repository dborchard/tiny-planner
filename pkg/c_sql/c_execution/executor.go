package execution

import (
	"tiny_planner/pkg/c_sql/b_planner/plancore"
	"tiny_planner/pkg/c_sql/b_planner/planphysical"
)

func (b *ExecutorBuilder) buildDelete(v *plancore.Delete) (Executor, error) {
	return nil, nil
}

func (b *ExecutorBuilder) buildInsert(v *plancore.Insert) (Executor, error) {
	return nil, nil
}

func (b *ExecutorBuilder) buildSelection(v *planphysical.PhysicalSelection) (Executor, error) {
	return nil, nil
}

func (b *ExecutorBuilder) buildProjection(v *planphysical.PhysicalProjection) (Executor, error) {
	return nil, nil
}

func (b *ExecutorBuilder) buildTableReader(v *planphysical.PhysicalTableReader) (Executor, error) {
	return nil, nil
}
