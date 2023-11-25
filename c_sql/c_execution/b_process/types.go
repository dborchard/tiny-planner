package process

import (
	"context"
	batch "tiny_planner/c_sql/c_execution/a_containers/b_batch"
)

type Register struct {
	// InputBatch, stores the result of the previous operator.
	InputBatch *batch.Batch
}

type Process struct {
	Reg Register

	Ctx    context.Context
	Cancel context.CancelFunc
}

type ExecStatus int

const (
	ExecStop ExecStatus = iota
	ExecNext
)
