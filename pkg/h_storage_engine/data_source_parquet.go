package datasource

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/parquet-go/parquet-go"
	"io"
	"os"
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type ParquetDataSource struct {
	Filename string
	Sch      containers.Schema
}

func (ds *ParquetDataSource) LoadAndCacheSchema() containers.Schema {
	pf, f, err := openParquetFile(ds.Filename)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	var fields []arrow.Field
	for _, field := range pf.Schema().Fields() {
		switch field.Type().Kind() {
		case parquet.Int32:
			fields = append(fields, arrow.Field{Name: field.Name(), Type: arrow.PrimitiveTypes.Int32})
		case parquet.Int64:
			fields = append(fields, arrow.Field{Name: field.Name(), Type: arrow.PrimitiveTypes.Int64})
		default:
			panic(fmt.Sprintf("unsupported type %s", field.Type().Kind()))
		}
	}

	schema := containers.Schema{Schema: arrow.NewSchema(fields, nil)}
	ds.Sch = schema

	return schema
}

func (ds *ParquetDataSource) Schema() containers.Schema {
	return ds.Sch
}

func (ds *ParquetDataSource) Scan(projection []string, ctx execution.TaskContext) []containers.Batch {
	pf, f, err := openParquetFile(ds.Filename)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	var vectors []containers.IVector
	for _, rg := range pf.RowGroups() {
		schema := rg.Schema()
		for c, colDef := range schema.Fields() {
			if !parquetColumnIn(colDef, projection) {
				continue
			}
			vectors = append(vectors, parquetColumnToVector(colDef, rg.ColumnChunks()[c]))
		}
	}

	return []containers.Batch{{ds.Sch, vectors}}
}

func parquetColumnToVector(colDef parquet.Field, col parquet.ColumnChunk) containers.IVector {
	var colType arrow.DataType
	colData := make([]any, 0)

	pages := col.Pages()
	for {
		page, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}

		reader := page.Values()
		data := make([]parquet.Value, page.NumValues())
		_, err = reader.ReadValues(data)

		switch colDef.Type().Kind() {
		case parquet.Int32:
			colType = arrow.PrimitiveTypes.Int32
			for _, value := range data {
				colData = append(colData, value.Int32())
			}
		case parquet.Int64:
			colType = arrow.PrimitiveTypes.Int64
			for _, value := range data {
				colData = append(colData, value.Int64())
			}
		default:
			panic("unsupported type")
		}
	}
	return containers.NewVector(colType, len(colData), colData)
}

func parquetColumnIn(columnDef parquet.Field, projections []string) bool {
	if projections == nil {
		return true
	}
	res := false
	for _, col := range projections {
		if col == columnDef.Name() {
			res = true
		}
	}
	return res
}

func openParquetFile(file string) (*parquet.File, *os.File, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}

	stats, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}

	pf, err := parquet.OpenFile(f, stats.Size())
	if err != nil {
		return nil, nil, err
	}

	return pf, f, nil
}
