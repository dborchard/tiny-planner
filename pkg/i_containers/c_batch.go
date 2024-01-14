package containers

import (
	"fmt"
	"strings"
)

type Batch struct {
	Schema  ISchema
	Vectors []IVector
}

func (r *Batch) RowCount() int {
	return r.Vectors[0].Len()
}

func (r *Batch) ColumnCount() int {
	return len(r.Vectors)
}

func (r *Batch) Column(i int) IVector {
	return r.Vectors[i]
}

func (r *Batch) String() string {
	var sb strings.Builder
	sb.WriteString(r.Schema.String())
	sb.WriteRune('\n')

	for _, field := range r.Vectors {
		sb.WriteString(field.String())
		sb.WriteRune('\n')
	}
	return sb.String()
}

func (r *Batch) Shrink(sel IVector) *Batch {
	newVectors := make([]IVector, len(r.Vectors))
	for i, currVector := range r.Vectors {
		newVectors[i] = currVector.Shrink(sel)
	}
	return &Batch{Schema: r.Schema, Vectors: newVectors}
}

// TODO: replace this

func (r *Batch) StringTable() [][]string {
	data := make([][]string, 0)
	for rIdx := 0; rIdx < r.RowCount(); rIdx++ {
		row := make([]string, 0)
		for c := 0; c < r.ColumnCount(); c++ {
			row = append(row, fmt.Sprintf("%v", r.Column(c).GetValue(rIdx)))
		}
		data = append(data, row)
	}
	return data
}

//
//// TODO: replace this
//func (r *Batch) Rows() []Row {
//	rows := make([]Row, 0)
//	for rIdx := 0; rIdx < r.RowCount(); rIdx++ {
//		row := Row{schema: r.Schema}
//		for c := 0; c < r.ColumnCount(); c++ {
//			row.values = append(row.values, r.Column(c).GetValue(rIdx))
//		}
//		rows = append(rows, row)
//	}
//	return rows
//}
