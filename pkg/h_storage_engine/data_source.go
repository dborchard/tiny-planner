package datasource

import (
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type DataSource interface {
	Schema() containers.Schema
	LoadAndCacheSchema() containers.Schema
	Scan(projection []string, ctx execution.TaskContext) []containers.Batch
}

var _ DataSource = &CsvDataSource{}
var _ DataSource = &ParquetDataSource{}
