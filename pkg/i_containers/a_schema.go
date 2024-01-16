package containers

import "github.com/apache/arrow/go/v12/arrow"

type ISchema interface {
	Select(projection []string) ISchema
	IndexOf(name string) int
	String() string
	Fields() []arrow.Field
	GetArrowSchema() *arrow.Schema
}

var _ ISchema = Schema{}

type Schema struct {
	src *arrow.Schema
}

func NewSchema(fields []arrow.Field, metadata *arrow.Metadata) Schema {
	return Schema{arrow.NewSchema(fields, metadata)}
}

func (s Schema) Select(projection []string) ISchema {
	subFields := make([]arrow.Field, 0)
	for _, field := range s.src.Fields() {
		for _, columnName := range projection {
			if field.Name == columnName {
				subFields = append(subFields, field)
				break
			}
		}
	}
	newSchema := arrow.NewSchema(subFields, nil)
	return Schema{newSchema}
}

func (s Schema) IndexOf(columnName string) int {
	for i, field := range s.Fields() {
		if field.Name == columnName {
			return i
		}
	}
	return -1
}

func (s Schema) String() string {
	return s.src.String()
}

func (s Schema) Fields() []arrow.Field {
	return s.src.Fields()
}

func (s Schema) GetArrowSchema() *arrow.Schema {
	return s.src
}
