package engine

import (
	"tiny_planner/pkg/b_dataframe"
	physicalplan "tiny_planner/pkg/f_physicalplan"
	execution "tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
)

type ExecContext struct {
	SessionID string
	State     physicalplan.ExecState
}

func NewContext() *ExecContext {
	return &ExecContext{
		State: physicalplan.ExecState{
			QueryPlanner: physicalplan.DefaultQueryPlanner{},
			RuntimeEnv:   execution.NewRuntimeEnv(),
		},
	}
}

func (c *ExecContext) ReadCsv(path string, options datasource.CsvReadOptions) dataframe.IDataFrame {
	src := datasource.CsvDataSource{
		Filename:   path,
		HasHeaders: options.HasHeader,
		BatchSize:  1024,
	}

	df := dataframe.NewDataFrame(c.State)
	return df.Scan(path, &src, nil)
}

func (c *ExecContext) ReadParquet(path string) dataframe.IDataFrame {
	src := datasource.ParquetDataSource{
		Filename: path,
	}

	df := dataframe.NewDataFrame(c.State)
	return df.Scan(path, &src, nil)
}

//func (c *ExecContext) RegisterCsv(name string, tablePath string, options datasource.CsvReadOptions) {
//
//}
//
//func (c *ExecContext) Sql(sql string) dataframe.IDataFrame {
//	return nil
//}
