package function

import (
	"context"
	types "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/a_types"
	vector "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/b_vector"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

var supportedBuiltIns = map[string]builtinDefinition{
	"abs": {
		props: map[string]string{},
		overloads: []Overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int32.ToType()
				},
				builtinFnSignature: func() BuiltinFn {
					return func(parameters []*vector.Vector, result *vector.Vector, proc *process.Process, length int) error {
						return abs(parameters, result, proc, length)
					}
				},
			},
		},
	},
}

func GetFunctionById(ctx context.Context, fid string) (f Overload, err error) {
	return supportedBuiltIns[fid].overloads[0], nil
}
