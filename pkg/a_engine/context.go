package engine

import (
	"time"
	"tiny_planner/pkg/b_dataframe"
	physicalplan "tiny_planner/pkg/f_physicalplan"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

type ExecContext struct {
	SessionID string
	State     *physicalplan.ExecState
}

func NewContext() *ExecContext {
	sessionId := "session_" + time.Now().String()
	return &ExecContext{
		SessionID: sessionId,
		State:     physicalplan.NewExecState(sessionId),
	}
}

func (c *ExecContext) Parquet(path string, schema containers.ISchema) (dataframe.IDataFrame, error) {
	src, err := datasource.NewParquetDataSource(path, schema)
	if err != nil {
		return nil, err
	}

	df := dataframe.NewDataFrame(c.State)
	return df.Scan(path, src, nil), nil
}
