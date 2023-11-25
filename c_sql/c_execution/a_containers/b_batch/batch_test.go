package batch

import (
	"testing"
	types "tiny_planner/a_types"
	vector "tiny_planner/c_sql/c_execution/a_containers/a_vector"
)

func Test1(t *testing.T) {
	colCount := 2
	rowCount := 3

	bat := NewWithSize(colCount) // 2 columns
	bat.SetRowCount(rowCount)    // 3 rows

	col1 := vector.NewVec(types.T_int32.ToType())
	for i := 0; i < rowCount; i++ {
		_ = col1.Append(int32(i), false)
	}
	bat.Vecs[0] = col1

	col2 := vector.NewVec(types.T_int32.ToType())
	for i := 0; i < rowCount; i++ {
		_ = col2.Append(int32(i), false)
	}
	bat.Vecs[1] = col2

}
