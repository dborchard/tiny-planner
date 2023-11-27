package colexecoutput

import (
	"bytes"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

func (o *Output) Init(proc *process.Process) (err error) {
	return nil
}

func (o *Output) Next(proc *process.Process) (process.ExecStatus, error) {
	bat := proc.Next()
	if bat == nil {
		return process.ExecStop, nil
	}
	if bat.IsEmpty() {
		proc.SetBatch(bat)
		return process.ExecNext, nil
	}
	if err := o.Func(o.Data, bat); err != nil {
		return process.ExecStop, err
	}
	return process.ExecNext, nil
}

func (o *Output) String(arg any, buf *bytes.Buffer) {
	buf.WriteString("sql output")
}
