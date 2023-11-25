package batch

import (
	"bytes"
	"fmt"
	"tiny_planner/a_containers/b_vector"
)

type Batch struct {
	Attrs    []string
	Vecs     []*vector.Vector
	rowCount int
}

func NewWithSize(n int) *Batch {
	return &Batch{
		Vecs:     make([]*vector.Vector, n),
		Attrs:    make([]string, n),
		rowCount: 0,
	}
}

func (bat *Batch) SetRowCount(rowCount int) {
	bat.rowCount = rowCount
}

func (bat *Batch) GetRowCount() int {
	return bat.rowCount
}

func (bat *Batch) GetVector(pos uint32) *vector.Vector {
	return bat.Vecs[pos]
}

func (bat *Batch) SetVector(pos uint32, vec *vector.Vector) {
	bat.Vecs[pos] = vec
}

func (bat *Batch) IsEmpty() bool {
	return bat.rowCount == 0
}

func (bat *Batch) Dup() (*Batch, error) {
	rbat := NewWithSize(len(bat.Vecs))
	rbat.Attrs = bat.Attrs
	rbat.Vecs = bat.Vecs
	rbat.rowCount = bat.rowCount
	return rbat, nil
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, vec := range bat.Vecs {
		buf.WriteString(fmt.Sprintf("%d : %s\n", i, vec.String()))
	}
	return buf.String()
}
