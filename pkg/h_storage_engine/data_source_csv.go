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
	Sch        containers.Schema
	HasHeaders bool
	BatchSize  int
}

func (ds *CsvDataSource) LoadAndCacheSchema() containers.Schema {
	// 1. Open File
	file, err := os.Open(ds.Filename)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err = file.Close()
	}(file)

	// 2. Read CSV
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		panic(err)
	}

	// 3. Create Arrow Schema
	fields := make([]arrow.Field, len(header))
	for i, name := range header {
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}

	schema := containers.Schema{Schema: arrow.NewSchema(fields, nil)}
	ds.Sch = schema

	return schema
}

func (ds *CsvDataSource) Schema() containers.Schema {
	return ds.Sch
}

func (ds *CsvDataSource) Scan(proj []string, ctx execution.TaskContext) []containers.Batch {

	file, err := os.Open(ds.Filename)
	if err != nil {
		log.Fatal(err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	_, cols := ds.readCsvTable(file)

	var vectors []containers.IVector
	for _, col := range cols {
		vectors = append(vectors, containers.NewVector(arrow.BinaryTypes.String, len(col), col))
	}

	return []containers.Batch{{ds.Sch, vectors}}
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
