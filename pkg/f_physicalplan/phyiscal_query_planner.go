package physicalplan

import (
	"errors"
	logicalplan "tiny_planner/pkg/e_logical_plan"
	containers "tiny_planner/pkg/i_containers"
)

type QueryPlanner interface {
	CreatePhyExpr(e logicalplan.Expr, schema containers.ISchema) (Expr, error)
	CreatePhyPlan(lp logicalplan.LogicalPlan, state ExecState) (PhysicalPlan, error)
}

type DefaultQueryPlanner struct {
}

func (d DefaultQueryPlanner) CreatePhyExpr(e logicalplan.Expr, schema containers.ISchema) (Expr, error) {
	switch v := e.(type) {
	case logicalplan.Column:
		return ColumnExpression{Index: schema.IndexOf(v.Name)}, nil
	case logicalplan.LiteralInt64:
		return LiteralInt64Expression{Value: v.Val}, nil
	case logicalplan.LiteralFloat64:
		return LiteralFloat64Expression{Value: v.Val}, nil
	case logicalplan.LiteralString:
		return LiteralStringExpression{Value: v.Val}, nil
	case logicalplan.BooleanBinaryExpr:
		l, err := d.CreatePhyExpr(v.L, schema)
		if err != nil {
			return nil, err
		}
		r, err := d.CreatePhyExpr(v.R, schema)
		if err != nil {
			return nil, err
		}
		return BooleanBinaryExpr{L: l, R: r, Op: v.Op}, nil
	default:
		return nil, errors.New("not implemented")
	}
}

func (d DefaultQueryPlanner) CreatePhyPlan(lp logicalplan.LogicalPlan, state ExecState) (PhysicalPlan, error) {
	switch lp.(type) {
	case logicalplan.Scan:
		return d.createScan(lp.(logicalplan.Scan), state)
	case logicalplan.Projection:
		return d.createProjection(lp.(logicalplan.Projection), state)
	case logicalplan.Selection:
		return d.createSelection(lp.(logicalplan.Selection), state)
	case logicalplan.Aggregate:
		return d.createAggregate(lp.(logicalplan.Aggregate), state)
	default:
		return nil, errors.New("not implemented")
	}
}

func (d DefaultQueryPlanner) createScan(scan logicalplan.Scan, state ExecState) (PhysicalPlan, error) {
	return Scan{Source: scan.Source, Projection: scan.Projection}, nil
}

func (d DefaultQueryPlanner) createProjection(projection logicalplan.Projection, state ExecState) (PhysicalPlan, error) {
	input, err := d.CreatePhyPlan(projection.Input, state)
	if err != nil {
		return nil, err
	}

	proj := make([]Expr, len(projection.Proj))
	for i, e := range projection.Proj {
		schema, err := input.Schema()
		if err != nil {
			return nil, err
		}
		proj[i], err = d.CreatePhyExpr(e, schema)
		if err != nil {
			return nil, err
		}
	}

	schema, err := projection.Schema()
	if err != nil {
		return nil, err
	}
	return Projection{Input: input, Proj: proj, Sch: schema}, nil
}

func (d DefaultQueryPlanner) createSelection(selection logicalplan.Selection, state ExecState) (PhysicalPlan, error) {
	input, err := d.CreatePhyPlan(selection.Input, state)
	if err != nil {
		return nil, err
	}
	schema, err := input.Schema()
	if err != nil {
		return nil, err
	}

	filterExpr, err := d.CreatePhyExpr(selection.Filter, schema)
	if err != nil {
		return nil, err
	}
	return Selection{Input: input, Filter: filterExpr}, nil
}

func (d DefaultQueryPlanner) createAggregate(aggregate logicalplan.Aggregate, state ExecState) (PhysicalPlan, error) {
	return nil, errors.New("not implemented")
}
