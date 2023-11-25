package process

import (
	"context"
	"tiny_planner/pkg/a_containers/c_batch"
)

type Register struct {
	// InputBatch, stores the result of the previous operator.
	InputBatch *batch.batch
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
