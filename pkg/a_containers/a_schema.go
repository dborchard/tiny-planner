package containers

import "github.com/apache/arrow/go/v12/arrow"

type Schema struct {
	*arrow.Schema
}

func (s Schema) Select(projection []string) Schema {
	fields := make([]arrow.Field, 0)
	for _, columnName := range projection {
		field, ok := s.FieldsByName(columnName)
		if ok {
			fields = append(fields, field...)
		}
	}
	newSchema := arrow.NewSchema(fields, nil)
	return Schema{newSchema}
}

func (s Schema) IndexOf(name string) int {
	for i, field := range s.Fields() {
		if field.Name == name {
			return i
		}
	}
	return -1
}
