package execution

import (
	"bytes"
	"context"
	"github.com/juju/errors"
	"time"
	catalog "tiny_planner/b_catalog"
	"tiny_planner/c_sql/b_planner/plancore"
	"tiny_planner/c_sql/b_planner/planphysical"
	process "tiny_planner/c_sql/c_execution/a_process"
)

type Executor interface {
	Prepare(proc *process.Process, arg any) (err error)
	Call(proc *process.Process, arg any) (process.ExecStatus, error)
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
		return nil, errors.Errorf("invalid plan type %T", v)
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
	return nil, nil
}

func (b *ExecutorBuilder) buildTableReader(v *planphysical.PhysicalTableReader) (Executor, error) {
	return nil, nil
}
