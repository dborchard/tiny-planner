package datasource

import (
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type TableReader interface {
	Schema() (containers.ISchema, error)
	Scan(projection []string, ctx execution.TaskContext) ([]containers.Batch, error)

	// Seek(predicate logicalplan.LogicalExpr) Iterator
	// Iterator() Iterator
	// HasNext() bool
	// Next() containers.Batch
}

var _ TableReader = &CsvDataSource{}
var _ TableReader = &ParquetDataSource{}
