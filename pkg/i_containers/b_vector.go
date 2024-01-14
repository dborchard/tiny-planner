package containers

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type IVector interface {
	DataType() arrow.DataType
	GetValue(i int) any
	Len() int
	String() string
	Shrink(sel IVector) IVector
}

var _ IVector = ConstVector{}
var _ IVector = Vector{}

// -----------------ConstVector------------------

func NewConstVector(arrowType arrow.DataType, size int, value any) ConstVector {
	return ConstVector{
		ArrowType: arrowType,
		Value:     value,
		Size:      size,
	}

}

type ConstVector struct {
	ArrowType arrow.DataType
	Value     any
	Size      int
}

func (c ConstVector) String() string {
	return fmt.Sprintf("ConstVector{ArrowType: %s, Value: %v, Size: %d}", c.ArrowType, c.Value, c.Size)
}

func (c ConstVector) DataType() arrow.DataType {
	return c.ArrowType
}

func (c ConstVector) GetValue(i int) any {
	if i < 0 || i >= c.Size {
		panic(fmt.Sprintf("index out of bounds %d vecsize: %d", i, c.Size))
	}
	return c.Value
}

func (c ConstVector) Len() int {
	return c.Size
}

func (c ConstVector) Shrink(sel IVector) IVector {
	//TODO: optimize
	var filteredCol []any
	for i := 0; i < sel.Len(); i++ {
		if sel.GetValue(i).(bool) {
			filteredCol = append(filteredCol, c.GetValue(i))
		}
	}
	return NewVector(c.DataType(), filteredCol)
}

// -----------------Vector------------------

type Vector struct {
	src arrow.Array
}

func NewVector(arrowType arrow.DataType, data []any) Vector {
	allocator := memory.NewGoAllocator()
	builder := array.NewBuilder(allocator, arrowType)
	defer builder.Release()

	switch arrowType.(type) {
	case *arrow.Int64Type:
		intBuilder := builder.(*array.Int64Builder)
		for _, value := range data {
			v, ok := value.(int64)
			if !ok {
				panic("unsupported type")
			}
			intBuilder.Append(v)
		}
	case *arrow.Float64Type:
		floatBuilder := builder.(*array.Float64Builder)
		for _, value := range data {
			v, ok := value.(float64)
			if !ok {
				panic("unsupported type")
			}
			floatBuilder.Append(v)
		}
	case *arrow.BooleanType:
		boolBuilder := builder.(*array.BooleanBuilder)
		for _, value := range data {
			v, ok := value.(bool)
			if !ok {
				panic("unsupported type")
			}
			boolBuilder.Append(v)
		}
	case *arrow.StringType:
		stringBuilder := builder.(*array.StringBuilder)
		for _, value := range data {
			v, ok := value.(string)
			if !ok {
				panic("unsupported type")
			}
			stringBuilder.Append(v)
		}
	default:
		panic("unsupported type")
	}

	dataArr := builder.NewArray()
	return Vector{src: dataArr}
}

func (v Vector) DataType() arrow.DataType {
	return v.src.DataType()
}

func (v Vector) GetValue(i int) any {
	return v.src.GetOneForMarshal(i)
}

func (v Vector) Len() int {
	return v.src.Len()
}

func (v Vector) Shrink(sel IVector) IVector {
	//TODO: optimize
	var filteredCol []any
	for i := 0; i < sel.Len(); i++ {
		if sel.GetValue(i).(bool) {
			filteredCol = append(filteredCol, v.GetValue(i))
		}
	}
	return NewVector(v.DataType(), filteredCol)
}

func (v Vector) String() string {
	return v.src.String()
}
