package dataframe

import (
	"tiny_planner/pkg/a_datafusion/common"
	"tiny_planner/pkg/a_datafusion/core/datasource"
	"tiny_planner/pkg/a_datafusion/exprLogi"
	"tiny_planner/pkg/a_datafusion/exprPhy"
)

type SessionContext struct {
	SessionID string
	State     SessionState
}

func New() *SessionContext {
	return &SessionContext{}
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
		Sch:        common.Schema{Schema: schema},
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

type QueryPlanner interface {
	CreatePhysicalPlan(lp exprLogi.LogicalPlan, state SessionState) exprPhy.PhysicalPlan
}
