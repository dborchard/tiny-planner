package containers

type IRow interface {
	Schema() Schema
	Values() []interface{}
}

type Row struct {
	schema Schema
	values []interface{}
}
