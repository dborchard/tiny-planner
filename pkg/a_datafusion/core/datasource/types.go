package datasource

import "tiny_planner/pkg/a_datafusion/common"

type DataSource interface {
	Schema() common.DFSchema
	Scan(projection []string) []common.Batch
}

var _ DataSource = &CsvDataSource{}
