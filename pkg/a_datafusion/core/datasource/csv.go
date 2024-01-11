package datasource

import "tiny_planner/pkg/a_datafusion/common"

type CsvDataSource struct {
	Filename   string
	DFSchema   common.DFSchema
	HasHeaders bool
	BatchSize  int
}

func (ds *CsvDataSource) Schema() common.DFSchema {
	return ds.DFSchema
}

func (ds *CsvDataSource) Scan(proj []string) []common.Batch {
	return []common.Batch{{ds.DFSchema, []common.ColumnVector{}}}
}

type CsvReadOptions struct {
	HasHeader bool
}
