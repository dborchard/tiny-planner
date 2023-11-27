package colexecoutput

import (
	execution "tiny_planner/pkg/c_sql/c_exec_engine"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
)

type Output struct {
	Data any
	Func func(any, *batch.Batch) error
}

var _ execution.Executor = new(Output)
