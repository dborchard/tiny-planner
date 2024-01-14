package datasource

import (
	"encoding/csv"
	"github.com/apache/arrow/go/v12/arrow"
	"io"
	"log"
	"os"
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type CsvDataSource struct {
	Filename   string
	Sch        containers.ISchema
	HasHeaders bool
	BatchSize  int
}

func (ds *CsvDataSource) Schema() (containers.ISchema, error) {
	if ds.Sch == nil {
		return ds.loadAndCacheSchema()
	}
	return ds.Sch, nil
}

func (ds *CsvDataSource) loadAndCacheSchema() (containers.ISchema, error) {
	// 1. Open File
	file, err := os.Open(ds.Filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 2. Read CSV
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// 3. Create Arrow ISchema
	fields := make([]arrow.Field, len(header))
	for i, name := range header {
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}

	schema := containers.NewSchema(fields, nil)
	ds.Sch = schema

	return schema, nil
}

func (ds *CsvDataSource) Scan(proj []string, ctx execution.TaskContext) ([]containers.Batch, error) {

	file, err := os.Open(ds.Filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	_, cols := ds.readCsvTable(file)

	var vectors []containers.IVector
	for _, col := range cols {
		vectors = append(vectors, containers.NewVector(arrow.BinaryTypes.String, col))
	}

	return []containers.Batch{{ds.Sch, vectors}}, nil
}

func (ds *CsvDataSource) readCsvTable(f *os.File) (header []string, data [][]any) {
	r := csv.NewReader(f)

	var err error
	header, err = r.Read()
	if err != nil {
		log.Fatal(err)
	}

	cols := make([][]any, len(header))
	for i := range cols {
		cols[i] = []any{}
	}

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		for i, val := range record {
			cols[i] = append(cols[i], val)
		}
	}
	return header, cols
}

type CsvReadOptions struct {
	HasHeader bool
}
