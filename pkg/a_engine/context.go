package engine

import (
	"time"
	"tiny_planner/pkg/b_dataframe"
	physicalplan "tiny_planner/pkg/f_physicalplan"
	datasource "tiny_planner/pkg/h_storage_engine"
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

func (c *ExecContext) Parquet(path string) dataframe.IDataFrame {
	src := datasource.ParquetDataSource{
		Filename: path,
	}

	df := dataframe.NewDataFrame(c.State)
	return df.Scan(path, &src, nil)
}
