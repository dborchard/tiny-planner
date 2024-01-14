package datasource

import (
	"context"
	"github.com/apache/arrow/go/v12/arrow"
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type TableReader interface {
	Schema() (containers.ISchema, error)
	Iterator(projection []string, ctx execution.TaskContext) ([]containers.IBatch, error)

	// Seek(predicate logicalplan.LogicalExpr) Iterator
	// Iterator() Iterator
	// HasNext() bool
	// Next() containers.Batch
}

var _ TableReader = &CsvDataSource{}
var _ TableReader = &ParquetDataSource{}

type Callback func(ctx context.Context, r arrow.Record) error
