package datasource

import (
	execution "tiny_planner/pkg/h_exec_runtime"
	containers "tiny_planner/pkg/j_containers"
)

type DataSource interface {
	Schema() containers.Schema
	Scan(projection []string, ctx execution.TaskContext) []containers.Batch
}

var _ DataSource = &CsvDataSource{}
