package containers

import "github.com/apache/arrow/go/v12/arrow"

type ISchema interface {
	Select(projection []string) (ISchema, error)
	IndexOf(name string) int
	String() string
	Fields() []arrow.Field
}

type Schema struct {
	*arrow.Schema
}

func NewSchema(fields []arrow.Field, metadata *arrow.Metadata) Schema {
	return Schema{arrow.NewSchema(fields, metadata)}
}

func (s Schema) Select(projection []string) (ISchema, error) {
	fields := make([]arrow.Field, 0)
	for _, columnName := range projection {
		field, ok := s.FieldsByName(columnName)
		if ok {
			fields = append(fields, field...)
		}
	}
	newSchema := arrow.NewSchema(fields, nil)
	return Schema{newSchema}, nil
}

func (s Schema) IndexOf(name string) int {
	for i, field := range s.Fields() {
		if field.Name == name {
			return i
		}
	}
	return -1
}
