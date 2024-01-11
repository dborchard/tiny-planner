package arrow_array

import (
	"tiny_planner/a/engine"
	logicalplan2 "tiny_planner/pkg/a_datafusion/expr/logicalplan"
)

// -------------------

type ExecutionContext struct{}

func (ec *ExecutionContext) Csv(filename string) IDataFrame {
	return &DataFrame{plan: logicalplan2.Scan{Path: filename, Source: &engine.CsvDataSource{Filename: filename}, Projection: []string{}}}
}
