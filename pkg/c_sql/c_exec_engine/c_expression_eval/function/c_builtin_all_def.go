package function

import (
	"math"
	types "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/a_types"
	vector "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/b_vector"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

func abs(parameters []*vector.Vector, result *vector.Vector, proc *process.Process, length int) error {

	switch parameters[0].GetType().Oid {
	case types.T_int32:
		err := absGeneric[int32](parameters, result, length)
		if err != nil {
			return err
		}

	case types.T_int64:
		err := absGeneric[int64](parameters, result, length)
		if err != nil {
			return err
		}

	}
	return nil
}

func absGeneric[T types.FixedSizeT](parameters []*vector.Vector, result *vector.Vector, length int) error {
	for i := 0; i < length; i++ {
		v, null := vector.Get[T](parameters[0], uint32(i))
		if null {
			if err := result.Append(0, true); err != nil {
				return err
			}
		} else {
			ans := math.Abs(float64(v))
			if err := result.Append(T(ans), false); err != nil {
				return err
			}
		}
	}

	return nil
}
