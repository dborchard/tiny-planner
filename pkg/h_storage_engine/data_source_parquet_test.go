package datasource

import (
	"context"
	"fmt"
	"testing"
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

func TestParquetDataSource_Scan(t *testing.T) {
	ds := ParquetDataSource{Filename: "../../test/data/c1_c2_int64.parquet"}
	ds.loadAndCacheSchema()

	err := ds.Iterator([]string{"c1", "c2"}, execution.TaskContext{
		Ctx: context.Background(),
	}, []Callback{func(ctx context.Context, r containers.IBatch) error {
		fmt.Println(r)
		return nil
	}})

	if err != nil {
		t.Error(err)
	}
}
