package colexecproj

import (
	execution "tiny_planner/pkg/c_sql/c_exec_engine"
	expression "tiny_planner/pkg/c_sql/c_exec_engine/c_expression_eval"
)

type ProjectionExec struct {
	Expressions []expression.Expr
	ctr         *container
}

type container struct {
	projExecutors []expression.Executor
}

var _ execution.Executor = new(ProjectionExec)
