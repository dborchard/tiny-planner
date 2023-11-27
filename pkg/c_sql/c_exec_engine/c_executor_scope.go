package execution

import (
	api "tiny_planner/pkg/c_sql/c_exec_engine/d_api"
	process "tiny_planner/pkg/c_sql/d_exec_runtime/a_process"
)

type Scope struct {
	Engine     api.StorageEngine
	DataSource *Source

	Process      *process.Process
	Instructions []Executor

	affectedRows uint64
}

type Source struct {
	SchemaName   string
	RelationName string
	Attributes   []string
	Reader       api.StorageEngineReader
}

func (s *Scope) CreateTable() error {
	dbName := ""
	return s.Engine.CreateTable(nil, dbName)
}

func (s *Scope) Run() error {
	p := NewPipeline(s.DataSource.Attributes, s.Instructions)
	if _, err := p.Run(s.DataSource.Reader, s.Process); err != nil {
		return err
	}
	return nil
}

func (s *Scope) AffectedRows() uint64 {
	return s.affectedRows
}
