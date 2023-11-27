package expression

import (
	"errors"
	vector "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/b_vector"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
	"tiny_planner/pkg/c_sql/c_exec_engine/c_expression_eval/function"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type Executor interface {
	Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error)
	Free()
}

func NewExecutorsFromPlanExpressions(proc *process.Process, planExprs []Expr) (executors []Executor, err error) {
	executors = make([]Executor, len(planExprs))
	for i := range executors {
		executors[i], err = NewExecutor(proc, planExprs[i])
		if err != nil {
			for j := 0; j < i; j++ {
				executors[j].Free()
			}
			return nil, err
		}
	}
	return executors, err
}

func NewExecutor(proc *process.Process, planExpr Expr) (Executor, error) {
	switch t := planExpr.(type) {
	case *ExprCol:
		typ := planExpr.(*ExprCol).Type
		return &ColumnExpressionExecutor{
			colIdx: t.ColIdx,
			typ:    typ,
		}, nil

	case *ExprFunc:
		overload, err := function.GetFunctionById(proc.Ctx, t.Name)
		if err != nil {
			return nil, err
		}

		executor := &FunctionExpressionExecutor{}
		typ := planExpr.(*ExprFunc).Type
		if err = executor.Init(proc, len(t.Args), typ, overload.GetBuiltinFn()); err != nil {
			return nil, err
		}

		for i := range executor.parameterExecutor {
			subExecutor, paramErr := NewExecutor(proc, t.Args[i])
			if paramErr != nil {
				for j := 0; j < i; j++ {
					executor.parameterExecutor[j].Free()
				}
				return nil, paramErr
			}
			executor.SetParameter(i, subExecutor)
		}

		return executor, nil
	}

	return nil, errors.New("unsupported executor")
}
