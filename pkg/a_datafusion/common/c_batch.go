package common

// Batch represents a batch of columnar data.
type Batch struct {
	Schema DFSchema
	Fields []ColumnVector
}
