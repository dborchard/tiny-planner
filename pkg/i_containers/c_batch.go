package containers

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"strings"
)

type IBatch interface {
	Schema() ISchema
	RowCount() int
	ColumnCount() int
	Column(i int) IVector
	Shrink(sel IVector)
	String() string
	StringTable() [][]string
}

type Batch struct {
	src arrow.Record
}

func (r *Batch) Schema() ISchema {
	return &Schema{src: r.src.Schema()}
}

func NewBatch(schema ISchema, vectors []IVector) IBatch {
	columns := make([]arrow.Array, len(vectors))
	for i, vector := range vectors {
		columns[i] = vector.GetArrowArray()
	}
	return &Batch{src: array.NewRecord(schema.GetArrowSchema(), columns, -1)}

}

func (r *Batch) RowCount() int {
	return int(r.src.NumRows())
}

func (r *Batch) ColumnCount() int {
	return int(r.src.NumCols())
}

func (r *Batch) Column(i int) IVector {
	return &Vector{src: r.src.Column(i)}
}

func (r *Batch) String() string {
	var sb strings.Builder
	for i := 0; i < r.ColumnCount(); i++ {
		sb.WriteString(fmt.Sprintf("%v", r.Column(i)))
		sb.WriteString("\n")
	}
	return sb.String()
}

func (r *Batch) Shrink(sel IVector) {
	allocator := memory.NewGoAllocator()

	selIndices := make(map[int]bool)
	for i := 0; i < sel.Len(); i++ {
		selIndices[i] = sel.GetValue(i).(bool)
	}

	schema := r.src.Schema()
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(allocator, field.Type)
	}

	// Iterate over each row in the original record.
	for rowIdx := 0; rowIdx < r.RowCount(); rowIdx++ {
		if !selIndices[rowIdx] {
			continue // Skip this row
		}

		// Append the value from each column to the respective builder.
		for colIdx := 0; colIdx < len(builders); colIdx++ {
			switch builders[colIdx].(type) {
			case *array.Int64Builder:
				builders[colIdx].(*array.Int64Builder).Append(r.Column(colIdx).GetValue(rowIdx).(int64))
			case *array.Float64Builder:
				builders[colIdx].(*array.Float64Builder).Append(r.Column(colIdx).GetValue(rowIdx).(float64))
			case *array.StringBuilder:
				builders[colIdx].(*array.StringBuilder).Append(r.Column(colIdx).GetValue(rowIdx).(string))
			case *array.BooleanBuilder:
				builders[colIdx].(*array.BooleanBuilder).Append(r.Column(colIdx).GetValue(rowIdx).(bool))
			default:
				panic("unsupported type")
			}
		}
	}

	// Create new arrays from builders and construct the new record.
	newArrays := make([]arrow.Array, len(builders))
	for i := range builders {
		newArrays[i] = builders[i].NewArray()
	}
	newRecord := array.NewRecord(schema, newArrays, int64(newArrays[0].Len()))
	r.src = newRecord
}

// TODO: replace this

func (r *Batch) StringTable() [][]string {
	data := make([][]string, 0)
	for rIdx := 0; rIdx < r.RowCount(); rIdx++ {
		row := make([]string, 0)
		for c := 0; c < r.ColumnCount(); c++ {
			row = append(row, fmt.Sprintf("%v", r.Column(c).GetArrowArray().ValueStr(rIdx)))
		}
		data = append(data, row)
	}
	return data
}
