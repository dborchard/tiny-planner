package engine

import (
	"time"
	"tiny_planner/pkg/b_dataframe"
	physicalplan "tiny_planner/pkg/d_physicalplan"
	datasource "tiny_planner/pkg/f_storage_engine"
	containers "tiny_planner/pkg/g_containers"
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

	return dataframe.NewDataFrame(c.State).Scan(path, src, nil), nil
}
