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
	var visitErr error
	var pPlan PhysicalPlan
	var prev PhysicalPlan
	lp.Accept(PostPlanVisitorFunc(func(plan logicalplan.LogicalPlan) bool {
		switch lPlan := plan.(type) {
		case logicalplan.Scan:
			pPlan = &Scan{Source: lPlan.Source, Projection: lPlan.Projection}
			prev = pPlan
		case logicalplan.Projection:
			proj := make([]Expr, len(lPlan.Proj))
			for i, e := range lPlan.Proj {
				schema, _ := prev.Schema()
				proj[i], _ = d.CreatePhyExpr(e, schema)
			}
			schema, _ := lPlan.Schema()

			projection := &Projection{Proj: proj, Sch: schema}
			prev = projection

			pPlan.SetNext(projection)
		case logicalplan.Selection:
			schema, _ := prev.Schema()
			filterExpr, _ := d.CreatePhyExpr(lPlan.Filter, schema)

			selection := &Selection{Filter: filterExpr}
			prev = selection

			pPlan.SetNext(selection)
		default:
			visitErr = errors.New("not implemented")
		}
		return visitErr == nil
	}))

	return pPlan, visitErr
}
