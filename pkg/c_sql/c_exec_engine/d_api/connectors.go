package api

import (
	"context"
	batch "tiny_planner/pkg/c_sql/c_exec_engine/a_coldata/c_batch"
)

type StorageEngine interface {
	Create(ctx context.Context, name string) error
}

type StorageEngineReader interface {
	Read(context.Context, []string) (*batch.Batch, error)
	Close() error
}
