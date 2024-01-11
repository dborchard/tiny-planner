package datasource

import (
	execution "tiny_planner/pkg/i_exec_runtime"
	containers "tiny_planner/pkg/k_containers"
)

type DataSource interface {
	Schema() containers.Schema
	Scan(projection []string, ctx execution.TaskContext) []containers.Batch
}

var _ DataSource = &CsvDataSource{}
