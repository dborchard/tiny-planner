package function

import (
	types "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/a_types"
	vector "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/b_vector"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type BuiltinFn func(parameters []*vector.Vector, result *vector.Vector, proc *process.Process, length int) error

type builtinDefinition struct {
	props     map[string]string
	overloads []Overload
}

type Overload struct {
	overloadId int
	args       []types.T

	retType            func(parameters []types.Type) types.Type
	builtinFnSignature func() BuiltinFn
}

func (ov *Overload) GetBuiltinFn() BuiltinFn {
	f := ov.builtinFnSignature
	return f()
}
