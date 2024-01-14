package datasource

import (
	"fmt"
	"testing"
	execution "tiny_planner/pkg/g_exec_runtime"
)

func TestParquetDataSource_Scan(t *testing.T) {
	ds := ParquetDataSource{Filename: "../../test/data/c1_c2_int32.parquet"}
	ds.loadAndCacheSchema()

	res, _ := ds.Iterator([]string{"c1", "c2"}, execution.TaskContext{})
	for _, batch := range res {
		fmt.Println(batch)
	}
}
