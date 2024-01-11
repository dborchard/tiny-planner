package arrow_array

import (
	"github.com/apache/arrow/go/v12/arrow"
	"tiny_planner/pkg/core/common"
)

// RecordBatch represents a batch of columnar data.
type RecordBatch struct {
	Schema common.DFSchema
	Fields []ColumnVector
}

type ColumnVector interface {
	DataType() arrow.DataType
	GetValue(i int) any
	Len() int
}

type DataSource interface {
	GetSchema() common.DFSchema
	Scan(projection []string) []RecordBatch
}
