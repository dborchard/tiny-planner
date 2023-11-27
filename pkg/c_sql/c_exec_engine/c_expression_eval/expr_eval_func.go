package expression

import (
	types "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/a_types"
	vector "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/b_vector"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
	"tiny_planner/pkg/c_sql/c_exec_engine/c_expression_eval/function"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type FunctionExpressionExecutor struct {
	ResultVector *vector.Vector

	parameterResults  []*vector.Vector
	parameterExecutor []Executor

	evalFn function.BuiltinFn
}

func (expr *FunctionExpressionExecutor) Init(_ *process.Process, parameterNum int, retType types.Type, fn function.BuiltinFn) (err error) {
	expr.evalFn = fn
	expr.parameterResults = make([]*vector.Vector, parameterNum)
	expr.parameterExecutor = make([]Executor, parameterNum)

	expr.ResultVector = vector.NewVec(retType)
	return err
}

func (expr *FunctionExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	var err error
	for i := range expr.parameterExecutor {
		expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches)
		if err != nil {
			return nil, err
		}
	}

	if err = expr.evalFn(expr.parameterResults, expr.ResultVector, proc, batches[0].GetRowCount()); err != nil {
		return nil, err
	}
	return expr.ResultVector, nil
}

func (expr *FunctionExpressionExecutor) Free() {
	for _, p := range expr.parameterExecutor {
		p.Free()
	}
}

func (expr *FunctionExpressionExecutor) SetParameter(index int, executor Executor) {
	expr.parameterExecutor[index] = executor
}
