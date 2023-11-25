package execution

import (
	"bytes"
	"context"
	"fmt"
	"github.com/blastrain/vitess-sqlparser/tidbparser/dependency/mysql"
	"github.com/juju/errors"
	"time"
	"tiny_planner/pkg/b_catalog"
	plancore2 "tiny_planner/pkg/c_sql/b_planner/plancore"
	"tiny_planner/pkg/c_sql/b_planner/planphysical"
	"tiny_planner/pkg/c_sql/d_exec_runtime/b_process"
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

func (b *ExecutorBuilder) Build(p plancore2.Plan) (Executor, error) {
	switch v := p.(type) {
	case *plancore2.Delete:
		return b.buildDelete(v)
	case *plancore2.Insert:
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

type InsertExec struct {
	Priority mysql.PriorityEnum
}
