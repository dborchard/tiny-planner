package batch

import (
	"fmt"
	"tiny_planner/pkg/a_common/a_types"
	"tiny_planner/pkg/c_sql/c_exec_engine/a_containers/a_vector"
)

func MockBatch(colCnt int, rowCnt int, rowStart int) *Batch {
	bat := NewWithSize(colCnt)
	bat.rowCount = rowCnt

	for i := 0; i < colCnt; i++ {
		bat.Attrs[i] = fmt.Sprintf("%s%d", "mock_", i)

		switch i % 20 {
		case 0:
			bat.Vecs[i] = vector.NewVec(types.T_int32.ToType())
			for j := rowStart; j < rowStart+rowCnt; j++ {
				_ = bat.Vecs[i].Append(int32(-j), false)
			}
		case 1:
			bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
			for j := rowStart; j < rowStart+rowCnt; j++ {
				_ = bat.Vecs[i].Append(int64(-j), false)
			}
		}

	}
	return bat
}
