package execution

import (
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
	api "tiny_planner/pkg/c_sql/c_exec_engine/d_api"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type Pipeline struct {
	colNames  []string //column list.
	executors []Executor
}

func NewPipeline(cols []string, execs []Executor) *Pipeline {
	return &Pipeline{
		colNames:  cols,
		executors: execs,
	}
}

func (p *Pipeline) Run(r api.StorageEngineReader, proc *process.Process) (end bool, err error) {

	var bat *batch.Batch
	if err = Init(p.executors, proc); err != nil {
		return false, err
	}

	for {
		select {
		case <-proc.Ctx.Done():
			proc.SetBatch(nil)
			return true, nil
		default:
		}
		// read data from storage engine
		if bat, err = r.Read(proc.Ctx, p.colNames); err != nil {
			return false, err
		}

		proc.SetBatch(bat)
		end, err = Run(p.executors, proc)
		if err != nil {
			return end, err
		}
		if end {
			return end, nil
		}
	}
}

func Init(executors []Executor, proc *process.Process) error {
	for _, executor := range executors {
		if err := executor.Init(proc); err != nil {
			return err
		}
	}
	return nil
}

func Run(executors []Executor, proc *process.Process) (end bool, err error) {
	var ok process.ExecStatus
	for _, executor := range executors {
		if ok, err = executor.Next(proc); err != nil {
			return ok == process.ExecStop, err
		}
		if ok == process.ExecStop {
			return true, nil
		}
	}
	return false, nil
}
