package datasource

import (
	"encoding/csv"
	"github.com/apache/arrow/go/v12/arrow"
	"io"
	"log"
	"os"
	execution "tiny_planner/pkg/h_exec_runtime"
	containers "tiny_planner/pkg/j_containers"
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
