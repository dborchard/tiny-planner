package engine

import (
	"github.com/apache/arrow/go/v12/arrow"
)

// RecordBatch represents a batch of columnar data.
type RecordBatch struct {
	Schema Schema
	Fields []ColumnVector
}

// struct embed arrow.Schema to add new methods + convenience
type Schema struct {
	*arrow.Schema
}

// abstraction on top of the arrow FieldVector
type ColumnVector interface {
	DataType() arrow.DataType
	GetValue(i int) any
	Len() int
}

type DataSource interface {
	GetSchema() Schema
	Scan(projection []string) []RecordBatch
}
