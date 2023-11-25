package vector

import (
	"fmt"
	"tiny_planner/pkg/a_containers/a_types"
)
import "github.com/RoaringBitmap/roaring"

type Vector struct {
	typ    *types.types
	col    any
	length int
	nsp    *roaring.Bitmap
}

func NewVec(typ types.Type) *Vector {
	v := &Vector{
		typ: &typ,
		nsp: roaring.New(),
	}

	switch typ.Oid {
	case types.T_int32:
		v.col = make([]int32, 0)
	case types.T_int64:
		v.col = make([]int64, 0)
	}
	return v
}

// Append strategy 1
func (v *Vector) Append(val any, isNull bool) error {
	length := v.length
	v.length++
	if isNull {
		v.nsp.Add(uint32(length))
	}
	switch v.typ.Oid {
	case types.T_int32:
		col := v.col.([]int32)
		col = append(col, val.(int32))
		v.col = col
	case types.T_int64:
		col := v.col.([]int64)
		col = append(col, val.(int64))
		v.col = col
	default:
		return fmt.Errorf("unsupport type %s", v.typ)
	}
	return nil
}

// AppendList append list when it contains no nulls.
// strategy 2
func AppendList[T any](vec *Vector, val []T) error {
	vec.length += len(val)

	col := vec.col.([]T)
	col = append(col, val...)
	vec.col = col

	return nil
}

func (v *Vector) Free() {
	v.nsp.Clear()
	v.col = nil
	v.length = 0
	v.typ = nil
}

func (v *Vector) Length() int {
	return v.length
}

func (v *Vector) GetType() *types.Type {
	return v.typ
}

func (v *Vector) String() string {
	switch v.typ.Oid {
	case types.T_int32:
		return vecToString[int32](v)
	case types.T_int64:
		return vecToString[int64](v)
	default:
		panic("vec to string unknown types.")
	}
}

func (v *Vector) Dup() (*Vector, error) {

	w := &Vector{
		typ:    v.typ,
		length: v.length,
		col:    v.col,
		nsp:    v.nsp.Clone(),
	}
	return w, nil
}

func (v *Vector) GetNsp() *roaring.Bitmap {
	return v.nsp
}

func vecToString[T types.FixedSizeT](v *Vector) string {
	col := v.col.([]T)
	if v.nsp.GetCardinality() > 0 {
		return fmt.Sprintf("%v-%s", col, v.nsp.String())
	} else {
		return fmt.Sprintf("%v", col)
	}
}

func Get[T any](vec *Vector, i uint32) (res T, isNull bool) {
	if vec.nsp.Contains(i) {
		return res, true
	}
	return vec.col.([]T)[i], false
}

func MustFixedCol[T any](v *Vector) []T {
	return v.col.([]T)
}
