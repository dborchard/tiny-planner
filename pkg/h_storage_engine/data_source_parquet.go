package datasource

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	execution "tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type ParquetDataSource struct {
	Filename string
	Sch      containers.ISchema
}

func (ds *ParquetDataSource) View(ctx context.Context, fn func(ctx context.Context, tx uint64) error) error {
	tx := uint64(0) // TODO: this is timestamp
	return fn(ctx, tx)
}

func (ds *ParquetDataSource) Schema() (containers.ISchema, error) {
	if ds.Sch == nil {
		return ds.loadAndCacheSchema()
	}
	return ds.Sch, nil
}

func (ds *ParquetDataSource) loadAndCacheSchema() (containers.ISchema, error) {
	pf, f, err := openParquetFile(ds.Filename)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	var fields []arrow.Field
	for _, field := range pf.Schema().Fields() {
		switch field.Type().Kind() {
		case parquet.Int32:
			fields = append(fields, arrow.Field{Name: field.Name(), Type: arrow.PrimitiveTypes.Int32})
		case parquet.Int64:
			fields = append(fields, arrow.Field{Name: field.Name(), Type: arrow.PrimitiveTypes.Int64})
		default:
			return nil, fmt.Errorf("unsupported type %s", field.Type().Kind())
		}
	}

	schema := containers.NewSchema(fields, nil)
	ds.Sch = schema

	return schema, nil
}

func (ds *ParquetDataSource) Iterator(projection []string, tCtx execution.TaskContext, callbacks []Callback) error {
	pf, f, err := openParquetFile(ds.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	rowGroups := make(chan parquet.RowGroup, len(callbacks))

	errG, ctx := errgroup.WithContext(tCtx.Ctx)
	for _, callback := range callbacks {
		callback := callback
		errG.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rg, ok := <-rowGroups:
					if !ok {
						return nil
					}
					var vectors []containers.IVector
					schema := rg.Schema()
					for c, colDef := range schema.Fields() {
						if !parquetColumnIn(colDef, projection) {
							continue
						}
						vector, err := parquetColumnToVector(colDef, rg.ColumnChunks()[c])
						if err != nil {
							return err
						}
						vectors = append(vectors, vector)
					}
					batch := containers.NewBatch(ds.Sch, vectors)
					err := callback(ctx, batch)
					if err != nil {
						return err
					}
				}
			}
		})
	}

	errG.Go(func() error {
		for _, rg := range pf.RowGroups() {
			rowGroups <- rg
		}
		close(rowGroups)
		return nil
	})

	return errG.Wait()
}

func parquetColumnToVector(colDef parquet.Field, col parquet.ColumnChunk) (containers.IVector, error) {
	var colType arrow.DataType
	colData := make([]any, 0)

	pages := col.Pages()
	for {
		page, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
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
			return nil, fmt.Errorf("unsupported type %s", colDef.Type().Kind())
		}
	}
	return containers.NewVector(colType, colData), nil
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
