package datasource

import containers "tiny_planner/pkg/a_containers"

type DataSource interface {
	Schema() containers.Schema
	Scan(projection []string) []containers.Batch
}

var _ DataSource = &CsvDataSource{}
