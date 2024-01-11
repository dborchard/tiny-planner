package common

// Batch represents a batch of columnar data.
type Batch struct {
	Schema Schema
	Fields []Vector
}

func (r *Batch) RowCount() int {
	return r.Fields[0].Len()
}

func (r *Batch) ColumnCount() int {
	return len(r.Fields)
}

func (r *Batch) Field(i int) Vector {
	return r.Fields[i]
}
