package dataframe

import (
	exprLogi "tiny_planner/pkg/f_logical_plan"
	execution "tiny_planner/pkg/h_exec_runtime"
	datasource "tiny_planner/pkg/i_storage_engine"
	containers "tiny_planner/pkg/j_containers"
)

type SessionContext struct {
	SessionID string
	State     SessionState
}

func NewContext() *SessionContext {
	return &SessionContext{
		State: SessionState{
			QueryPlanner: DefaultQueryPlanner{},
			RuntimeEnv:   execution.NewRuntimeEnv(),
		},
	}
}

func (c *SessionContext) ReadCsv(path string, options datasource.CsvReadOptions) IDataFrame {

	// read files from the path
	filePaths, err := datasource.ReadAllFiles(path)
	if err != nil {
		panic(err)
	}

	if len(filePaths) == 0 {
		panic("no files found")
	}

	schema, err := datasource.InferArrowSchemaFromCSV(filePaths[0])
	if err != nil {
		panic(err)
	}

	src := datasource.CsvDataSource{
		Filename:   path,
		Sch:        containers.Schema{Schema: schema},
		HasHeaders: options.HasHeader,
		BatchSize:  1024,
	}
	plan := exprLogi.Scan{Path: path, Source: &src, Projection: nil}
	return NewDataFrame(c.State, plan)
}

func (c *SessionContext) RegisterCsv(name string, tablePath string, options datasource.CsvReadOptions) {

}

func (c *SessionContext) Sql(sql string) IDataFrame {
	return nil
}
