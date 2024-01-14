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

func NewConstVector(arrowType arrow.DataType, value any, size int) ConstVector {
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
	//TODO: move to abstract class
	var filteredCol []any
	for i := 0; i < sel.Len(); i++ {
		if sel.GetValue(i).(bool) {
			filteredCol = append(filteredCol, c.GetValue(i))
		}
	}
	return NewVector(c.DataType(), len(filteredCol), filteredCol)
}

// -----------------Vector------------------

type Vector struct {
	dtype       arrow.DataType
	boolData    *array.Boolean
	int8Data    *array.Int8
	int16Data   *array.Int16
	int32Data   *array.Int32
	int64Data   *array.Int64
	float32Data *array.Float32
	float64Data *array.Float64
	stringData  *array.String
}

func (v Vector) String() string {
	switch v.dtype.(type) {
	case *arrow.BooleanType:
		return v.boolData.String()
	case *arrow.Int8Type:
		return v.int8Data.String()
	case *arrow.Int16Type:
		return v.int16Data.String()
	case *arrow.Int32Type:
		return v.int32Data.String()
	case *arrow.Int64Type:
		return v.int64Data.String()
	case *arrow.Float32Type:
		return v.float32Data.String()
	case *arrow.Float64Type:
		return v.float64Data.String()
	case *arrow.StringType:
		return v.stringData.String()
	default:
		panic("Unsupported Arrow type")
	}
}

func (v Vector) Len() int {
	switch v.dtype.(type) {
	case *arrow.BooleanType:
		return v.boolData.Len()
	case *arrow.Int8Type:
		return v.int8Data.Len()
	case *arrow.Int16Type:
		return v.int16Data.Len()
	case *arrow.Int32Type:
		return v.int32Data.Len()
	case *arrow.Int64Type:
		return v.int64Data.Len()
	case *arrow.Float32Type:
		return v.float32Data.Len()
	case *arrow.Float64Type:
		return v.float64Data.Len()
	case *arrow.StringType:
		return v.stringData.Len()
	default:
		panic("Unsupported Arrow type")
	}
}

func (v Vector) GetValue(i int) any {
	switch v.dtype.(type) {
	case *arrow.BooleanType:
		return v.boolData.Value(i)
	case *arrow.Int8Type:
		return v.int8Data.Value(i)
	case *arrow.Int16Type:
		return v.int16Data.Value(i)
	case *arrow.Int32Type:
		return v.int32Data.Value(i)
	case *arrow.Int64Type:
		return v.int64Data.Value(i)
	case *arrow.Float32Type:
		return v.float32Data.Value(i)
	case *arrow.Float64Type:
		return v.float64Data.Value(i)
	case *arrow.StringType:
		return v.stringData.Value(i)
	default:
		panic("Unsupported Arrow type")
	}
}

func (v Vector) DataType() arrow.DataType {
	return v.dtype
}

func (v Vector) Shrink(sel IVector) IVector {
	var filteredCol []any
	for i := 0; i < sel.Len(); i++ {
		if sel.GetValue(i).(bool) {
			filteredCol = append(filteredCol, v.GetValue(i))
		}
	}
	return NewVector(v.DataType(), len(filteredCol), filteredCol)
}

func NewVector(arrowType arrow.DataType, initialCapacity int, data []any) Vector {
	rootAllocator := memory.NewGoAllocator()
	out := Vector{dtype: arrowType}
	switch arrowType.(type) {
	case *arrow.BooleanType:
		vs := array.NewBooleanBuilder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(bool))
		}
		out.boolData = vs.NewBooleanArray()
	case *arrow.Int8Type:
		vs := array.NewInt8Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int8))
		}
		out.int8Data = vs.NewInt8Array()
	case *arrow.Int16Type:
		vs := array.NewInt16Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int16))
		}
		out.int16Data = vs.NewInt16Array()
	case *arrow.Int32Type:
		vs := array.NewInt32Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int32))
		}
		out.int32Data = vs.NewInt32Array()
	case *arrow.Int64Type:
		vs := array.NewInt64Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(int64))
		}
		out.int64Data = vs.NewInt64Array()
	case *arrow.Float32Type:
		vs := array.NewFloat32Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(float32))
		}
		out.float32Data = vs.NewFloat32Array()
	case *arrow.Float64Type:
		vs := array.NewFloat64Builder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(float64))
		}
		out.float64Data = vs.NewFloat64Array()
	case *arrow.StringType:
		vs := array.NewStringBuilder(rootAllocator)
		vs.Reserve(initialCapacity)
		for _, v := range data {
			vs.Append(v.(string))
		}
		out.stringData = vs.NewStringArray()
	default:
		panic("Unsupported Arrow type")
	}
	return out
}
