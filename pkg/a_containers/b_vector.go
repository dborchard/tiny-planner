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

func (v ConstVector) String() string {
	return fmt.Sprintf("ConstVector{ArrowType: %s, Value: %v, Size: %d}", v.ArrowType, v.Value, v.Size)
}

func (v ConstVector) DataType() arrow.DataType {
	return v.ArrowType
}

func (v ConstVector) GetValue(i int) any {
	if i < 0 || i >= v.Size {
		panic(fmt.Sprintf("index out of bounds %d vecsize: %d", i, v.Size))
	}
	return v.Value
}

func (v ConstVector) Len() int {
	return v.Size
}

// -----------------Vector------------------

var (
	Int8    = &arrow.Int8Type{}
	Int16   = &arrow.Int16Type{}
	Int32   = &arrow.Int32Type{}
	Int64   = &arrow.Int64Type{}
	Float32 = &arrow.Float32Type{}
	Float64 = &arrow.Float64Type{}
	String  = &arrow.StringType{}
	Boolean = &arrow.BooleanType{}
)

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

func (arr Vector) String() string {
	switch arr.dtype.(type) {
	case *arrow.BooleanType:
		return arr.boolData.String()
	case *arrow.Int8Type:
		return arr.int8Data.String()
	case *arrow.Int16Type:
		return arr.int16Data.String()
	case *arrow.Int32Type:
		return arr.int32Data.String()
	case *arrow.Int64Type:
		return arr.int64Data.String()
	case *arrow.Float32Type:
		return arr.float32Data.String()
	case *arrow.Float64Type:
		return arr.float64Data.String()
	case *arrow.StringType:
		return arr.stringData.String()
	default:
		panic("Unsupported Arrow type")
	}
}

func (arr Vector) Len() int {
	switch arr.dtype.(type) {
	case *arrow.BooleanType:
		return arr.boolData.Len()
	case *arrow.Int8Type:
		return arr.int8Data.Len()
	case *arrow.Int16Type:
		return arr.int16Data.Len()
	case *arrow.Int32Type:
		return arr.int32Data.Len()
	case *arrow.Int64Type:
		return arr.int64Data.Len()
	case *arrow.Float32Type:
		return arr.float32Data.Len()
	case *arrow.Float64Type:
		return arr.float64Data.Len()
	case *arrow.StringType:
		return arr.stringData.Len()
	default:
		panic("Unsupported Arrow type")
	}
}

func (arr Vector) GetValue(i int) any {
	switch arr.dtype.(type) {
	case *arrow.BooleanType:
		return arr.boolData.Value(i)
	case *arrow.Int8Type:
		return arr.int8Data.Value(i)
	case *arrow.Int16Type:
		return arr.int16Data.Value(i)
	case *arrow.Int32Type:
		return arr.int32Data.Value(i)
	case *arrow.Int64Type:
		return arr.int64Data.Value(i)
	case *arrow.Float32Type:
		return arr.float32Data.Value(i)
	case *arrow.Float64Type:
		return arr.float64Data.Value(i)
	case *arrow.StringType:
		return arr.stringData.Value(i)
	default:
		panic("Unsupported Arrow type")
	}
}

func (arr Vector) DataType() arrow.DataType {
	return arr.dtype
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
