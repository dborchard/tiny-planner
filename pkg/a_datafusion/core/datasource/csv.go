package datasource

import "tiny_planner/pkg/a_datafusion/common"

type CsvDataSource struct {
	Filename   string
	Sch        common.Schema
	HasHeaders bool
	BatchSize  int
}

func (ds *CsvDataSource) Schema() common.Schema {
	return ds.Sch
}

func (ds *CsvDataSource) Scan(proj []string) []common.Batch {
	
	return []common.Batch{{ds.Sch, []common.Vector{}}}
}

type CsvReadOptions struct {
	HasHeader bool
}
