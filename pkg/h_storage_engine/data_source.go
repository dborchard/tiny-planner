package datasource

import (
	"context"
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type TableReader interface {
	Schema() (containers.ISchema, error)
	View(ctx context.Context, fn func(ctx context.Context, tx uint64) error) error
	Iterator(projection []string, ctx execution.TaskContext, callbacks []Callback) error

	// Seek(predicate logicalplan.LogicalExpr) Iterator
	// Iterator() Iterator
	// HasNext() bool
	// Next() containers.Batch
}

var _ TableReader = &ParquetDataSource{}

type Callback func(ctx context.Context, r containers.IBatch) error
