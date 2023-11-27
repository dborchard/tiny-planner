package process

import (
	"context"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
)

type Register struct {
	// InputBatch, stores the result of the previous operator.
	InputBatch *batch.Batch
}

type ExecStatus int

const (
	ExecStop ExecStatus = iota
	ExecNext
)

// Process aka FeedOperator in CockroachDB, stores the execution data.
type Process struct {
	Reg Register

	Ctx    context.Context
	Cancel context.CancelFunc
}

// New creates a new Process.
// A process stores the execution context.
func New(ctx context.Context) *Process {
	return &Process{Ctx: ctx}
}

func (proc *Process) Next() *batch.Batch {
	return proc.Reg.InputBatch
}

func (proc *Process) SetBatch(bat *batch.Batch) {
	proc.Reg.InputBatch = bat
}
