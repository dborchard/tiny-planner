package catalog

import (
	"fmt"
	"tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/a_types"
)

func MockTableDef(tblName string, colCnt int) *TableDef {
	schema := TableDef{
		Name:    tblName,
		ColDefs: make([]*ColDef, 0),
	}

	prefix := "mock_"
	var typ types.Type

	for i := 0; i < colCnt; i++ {
		switch i % 20 {
		case 0:
			typ = types.T_int32.ToType()
		case 1:
			typ = types.T_int64.ToType()
		}

		name := fmt.Sprintf("%s%d", prefix, i)
		_ = schema.AppendCol(name, typ)
	}
	return &schema
}
