package common

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
)

type ColumnVector interface {
	DataType() arrow.DataType
	GetValue(i int) any
	Len() int
}

var _ ColumnVector = LiteralValueVector{}

type LiteralValueVector struct {
	arrowType arrow.DataType
	value     any
	size      int
}

func (v LiteralValueVector) DataType() arrow.DataType {
	return v.arrowType
}

func (v LiteralValueVector) GetValue(i int) any {
	if i < 0 || i >= v.size {
		panic(fmt.Sprintf("index out of bounds %d vecsize: %d", i, v.size))
	}
	return v.value
}

func (v LiteralValueVector) Len() int {
	return v.size
}
