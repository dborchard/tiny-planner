package dataframe

import (
	exprLogi "tiny_planner/pkg/d_exprLogi"
	exprPhy "tiny_planner/pkg/e_exprPhy"
)

type QueryPlanner interface {
	CreatePhysicalPlan(lp exprLogi.LogicalPlan, state SessionState) exprPhy.PhysicalPlan
}

type DefaultQueryPlanner struct {
}

func (d DefaultQueryPlanner) CreatePhysicalPlan(lp exprLogi.LogicalPlan, state SessionState) exprPhy.PhysicalPlan {
	switch lp.(type) {
	case exprLogi.Scan:
		return d.createScan(lp.(exprLogi.Scan), state)
	case exprLogi.Projection:
		return d.createProjection(lp.(exprLogi.Projection), state)
	case exprLogi.Selection:
		return d.createSelection(lp.(exprLogi.Selection), state)
	case exprLogi.Aggregate:
		return d.createAggregate(lp.(exprLogi.Aggregate), state)
	default:
		panic("not implemented")
	}
}

func (d DefaultQueryPlanner) createScan(scan exprLogi.Scan, state SessionState) exprPhy.PhysicalPlan {
	return exprPhy.ScanExec{Source: scan.Source, Projection: scan.Projection}
}

func (d DefaultQueryPlanner) createProjection(projection exprLogi.Projection, state SessionState) exprPhy.PhysicalPlan {
	childPlan := d.CreatePhysicalPlan(projection.Input, state)
	proj := make([]exprPhy.Expression, len(projection.Expr))
	for i, e := range projection.Expr {
		proj[i] = exprPhy.FromLogicalToPhysical(e, childPlan.Schema())
	}
	return exprPhy.ProjectionExec{Input: childPlan, Proj: proj, Sch: projection.Schema()}
}

func (d DefaultQueryPlanner) createSelection(selection exprLogi.Selection, state SessionState) exprPhy.PhysicalPlan {
	childPlan := d.CreatePhysicalPlan(selection.Input, state)
	return exprPhy.SelectionExec{Input: childPlan, Filter: exprPhy.FromLogicalToPhysical(selection.Filter, childPlan.Schema())}
}

func (d DefaultQueryPlanner) createAggregate(aggregate exprLogi.Aggregate, state SessionState) exprPhy.PhysicalPlan {
	panic("not implemented")
}
