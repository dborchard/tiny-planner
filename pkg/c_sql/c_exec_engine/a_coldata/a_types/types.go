package types

type T uint8

const (
	T_int32 T = iota
	T_int64
)

func (t T) ToType() Type {
	var typ Type

	typ.Oid = t
	switch t {
	case T_int32:
		typ.Size = 4
	case T_int64:
		typ.Size = 8
	default:
		panic("Unknown type")
	}
	return typ
}

type Type struct {
	Oid  T
	Size int32
}

func (t *Type) String() string {
	switch t.Oid {
	case T_int32:
		return "INT32"
	case T_int64:
		return "INT64"
	default:
		panic("Unknown type")
	}
}

type FixedSizeT interface {
	~int32 | ~int64
}
