package datasource

import (
	"context"
	"fmt"
	"testing"
	execution "tiny_planner/pkg/e_exec_runtime"
	containers "tiny_planner/pkg/g_containers"
)

func TestParquetDataSource_Scan(t *testing.T) {
	ds, err := NewParquetDataSource("../../test/data/c1_c2_int64.parquet", nil)
	if err != nil {
		t.Error(err)
	}

	err = ds.Iterator(execution.TaskContext{Ctx: context.Background()},
		[]Callback{func(ctx context.Context, r containers.IBatch) error {
			fmt.Println(r)
			return nil
		}},
		WithProjection("c1", "c2"))

	if err != nil {
		t.Error(err)
	}
}
