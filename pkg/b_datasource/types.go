package datasource

import (
	containers "tiny_planner/pkg/a_containers"
	execution "tiny_planner/pkg/d_exec_runtime"
)

type DataSource interface {
	Schema() containers.Schema
	Scan(projection []string, ctx execution.TaskContext) []containers.Batch
}

var _ DataSource = &CsvDataSource{}
