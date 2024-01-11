package datasource

import (
	"fmt"
	"testing"
	execution "tiny_planner/pkg/g_exec_runtime"
)

func TestParquetDataSource_Scan(t *testing.T) {
	ds := ParquetDataSource{Filename: "/Users/arjunsunilkumar/PycharmProjects/mo-benchmark-test/sample_int32.parquet"}
	ds.LoadAndCacheSchema()

	res := ds.Scan([]string{"c1", "c2"}, execution.TaskContext{})
	for _, batch := range res {
		fmt.Println(batch)
	}
}
