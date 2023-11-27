package expression

import (
	types "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/a_types"
	vector "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/b_vector"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type ColumnExpressionExecutor struct {
	typ    types.Type
	colIdx int
}

var _ Executor = new(ColumnExpressionExecutor)

func (expr *ColumnExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	vec := batches[0].Vecs[expr.colIdx]
	return vec, nil
}

func (expr *ColumnExpressionExecutor) Free() {
}
