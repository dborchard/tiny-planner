package colexecproj

import (
	"bytes"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
	expression "tiny_planner/pkg/c_sql/c_exec_engine/c_expression_eval"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

func (p *ProjectionExec) Init(proc *process.Process) (err error) {
	p.ctr.projExecutors, err = expression.NewExecutorsFromPlanExpressions(proc, p.Expressions)
	return err
}

func (p *ProjectionExec) Next(proc *process.Process) (process.ExecStatus, error) {

	bat := proc.Next()
	if bat == nil {
		proc.SetBatch(nil)
		return process.ExecStop, nil
	}
	if bat.IsEmpty() {
		return process.ExecNext, nil
	}

	// do projection and set the Batch to the process.
	resultBat := batch.NewWithSize(len(p.Expressions))
	for i := range p.ctr.projExecutors {
		vec, err := p.ctr.projExecutors[i].Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return process.ExecNext, err
		}
		resultBat.Vecs[i] = vec
	}
	resultBat.SetRowCount(bat.GetRowCount())
	proc.SetBatch(resultBat)

	return process.ExecNext, nil
}

func (p *ProjectionExec) String(arg any, buf *bytes.Buffer) {
	buf.WriteString("projection()")
}
