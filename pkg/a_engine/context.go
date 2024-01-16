package engine

import (
	"tiny_planner/pkg/b_dataframe"
	physicalplan "tiny_planner/pkg/f_physicalplan"
	execution "tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
)

type ExecContext struct {
	SessionID string
	State     *physicalplan.ExecState
}

func NewContext() *ExecContext {
	return &ExecContext{
		State: &physicalplan.ExecState{
			QueryPlanner: physicalplan.DefaultQueryPlanner{},
			RuntimeEnv:   execution.NewRuntimeEnv(),
		},
	}
}

func (c *ExecContext) Parquet(path string) dataframe.IDataFrame {
	src := datasource.ParquetDataSource{
		Filename: path,
	}

	df := dataframe.NewDataFrame(c.State)
	return df.Scan(path, &src, nil)
}
