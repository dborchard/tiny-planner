package common

import "github.com/apache/arrow/go/v12/arrow"

type DFSchema struct {
	*arrow.Schema
}

func (s DFSchema) Select(projection []string) DFSchema {
	fields := make([]arrow.Field, len(projection))
	for i, name := range projection {
		fields[i] = s.FieldWithName(name)
	}
	return DFSchema{arrow.NewSchema(fields, nil)}
}
