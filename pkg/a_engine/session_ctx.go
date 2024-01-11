package engine

import (
	"tiny_planner/pkg/b_dataframe"
	exprLogi "tiny_planner/pkg/e_logical_plan"
	exec "tiny_planner/pkg/f_exec_engine"
	execution "tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
)

type ExecContext struct {
	SessionID string
	State     exec.ExecState
}

func NewContext() *ExecContext {
	return &ExecContext{
		State: exec.ExecState{
			QueryPlanner: exec.DefaultQueryPlanner{},
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
	src.LoadAndCacheSchema()

	plan := exprLogi.Scan{Path: path, Source: &src, Projection: nil}
	return dataframe.NewDataFrame(c.State, plan)
}

func (c *ExecContext) ReadParquet(path string) dataframe.IDataFrame {
	src := datasource.ParquetDataSource{
		Filename: path,
	}
	src.LoadAndCacheSchema()

	plan := exprLogi.Scan{Path: path, Source: &src, Projection: nil}
	return dataframe.NewDataFrame(c.State, plan)
}

func (c *ExecContext) RegisterCsv(name string, tablePath string, options datasource.CsvReadOptions) {

}

func (c *ExecContext) Sql(sql string) dataframe.IDataFrame {
	return nil
}
