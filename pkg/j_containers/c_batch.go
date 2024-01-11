package containers

import "strings"

type Batch struct {
	Schema Schema
	Fields []IVector
}

func (r *Batch) RowCount() int {
	return r.Fields[0].Len()
}

func (r *Batch) ColumnCount() int {
	return len(r.Fields)
}

func (r *Batch) Column(i int) IVector {
	return r.Fields[i]
}

func (r *Batch) String() string {
	var sb strings.Builder
	sb.WriteString(r.Schema.String())
	sb.WriteRune('\n')

	for _, field := range r.Fields {
		sb.WriteString(field.String())
		sb.WriteRune('\n')
	}
	return sb.String()
}
