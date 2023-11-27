package execution

import (
	"bytes"
	"context"
	"fmt"
	"github.com/juju/errors"
	"time"
	"tiny_planner/pkg/b_catalog"
	plancore "tiny_planner/pkg/c_sql/b_planner/plancore"
	"tiny_planner/pkg/c_sql/b_planner/planphysical"
	"tiny_planner/pkg/c_sql/c_exec_engine/b_colexec/colexecproj"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type Executor interface {
	Init(proc *process.Process) (err error)
	Next(proc *process.Process) (process.ExecStatus, error)
	String(arg any, buf *bytes.Buffer)
}

type ExecutorBuilder struct {
	ctx     context.Context
	is      *catalog.TableDef
	startTS int64
}

func NewExecutorBuilder(ctx context.Context, is *catalog.TableDef) *ExecutorBuilder {
	return &ExecutorBuilder{
		ctx:     ctx,
		is:      is,
		startTS: time.Now().UnixNano(),
	}
}

func (b *ExecutorBuilder) Build(p plancore.Plan) (Executor, error) {
	switch v := p.(type) {
	case *plancore.Delete:
		return b.buildDelete(v)
	case *plancore.Insert:
		return b.buildInsert(v)
	case *planphysical.PhysicalSelection:
		return b.buildSelection(v)
	case *planphysical.PhysicalProjection:
		return b.buildProjection(v)
	case *planphysical.PhysicalTableReader:
		return b.buildTableReader(v)
	default:
		return nil, errors.New(fmt.Sprintf("invalid plan type %T", v))
	}
}

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
	e := &colexecproj.ProjectionExec{
		Expressions: v.Expressions,
	}
	return e, nil
}

func (b *ExecutorBuilder) buildTableReader(v *planphysical.PhysicalTableReader) (Executor, error) {
	return nil, nil
}
