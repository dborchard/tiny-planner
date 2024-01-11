package engine

import (
	"tiny_planner/pkg/core/arrow_array"
	"tiny_planner/pkg/core/common"
)

type CsvDataSource struct {
	Filename   string
	Schema     common.DFSchema
	hasHeaders bool
	batchSize  int
}

func (ds *CsvDataSource) GetSchema() common.DFSchema {
	return ds.Schema
}

func (ds *CsvDataSource) Scan(projection []string) []arrow_array.RecordBatch {
	return []arrow_array.RecordBatch{{ds.Schema, []arrow_array.ColumnVector{}}}
}
