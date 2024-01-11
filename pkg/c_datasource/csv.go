package datasource

import (
	"github.com/apache/arrow/go/v12/arrow"
	containers "tiny_planner/pkg/a_containers"
)

type CsvDataSource struct {
	Filename   string
	Sch        containers.Schema
	HasHeaders bool
	BatchSize  int
}

func (ds *CsvDataSource) Schema() containers.Schema {
	return ds.Sch
}

func (ds *CsvDataSource) Scan(proj []string) []containers.Batch {
	return []containers.Batch{{ds.Sch, []containers.Vector{
		containers.NewLiteralValueVector(arrow.BinaryTypes.String, []string{"a", "b", "c"}, 3),
		containers.NewLiteralValueVector(arrow.BinaryTypes.String, []string{"a", "b", "c"}, 3),
		containers.NewLiteralValueVector(arrow.BinaryTypes.String, []string{"a", "b", "c"}, 3),
	}}}
}

type CsvReadOptions struct {
	HasHeader bool
}
