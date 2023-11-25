package plancore

import (
	"context"
	"tiny_planner/b_catalog"
)

type Plan interface {
	Schema() *catalog.TableDef
	SetSchema(*catalog.TableDef)

	SCtx() context.Context

	ExplainInfo() string
}

type BasePlan struct {
	Id     int
	Ctx    context.Context
	schema *catalog.TableDef
}

func (s *BasePlan) Schema() *catalog.TableDef {
	return s.schema
}

func (s *BasePlan) SetSchema(schema *catalog.TableDef) {
	s.schema = schema
}

// ExplainInfo implements Plan interface.
func (s *BasePlan) ExplainInfo() string {
	return "N/A"
}

func (s *BasePlan) SCtx() context.Context {
	return s.Ctx
}

func NewBasePlan(ctx context.Context) BasePlan {
	return BasePlan{
		Id:  1,
		Ctx: ctx,
	}
}
