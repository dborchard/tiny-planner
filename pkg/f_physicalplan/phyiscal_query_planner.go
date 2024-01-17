package physicalplan

import (
	"errors"
	logicalplan "tiny_planner/pkg/e_logical_plan"
	"tiny_planner/pkg/f_physicalplan/expr_eval"
	"tiny_planner/pkg/f_physicalplan/operators"
	containers "tiny_planner/pkg/i_containers"
)

type QueryPlanner interface {
	CreatePhyExpr(e logicalplan.Expr, schema containers.ISchema) (expr_eval.Expr, error)
	CreatePhyPlan(lp logicalplan.LogicalPlan, state ExecState) (operators.PhysicalPlan, error)
}

type DefaultQueryPlanner struct {
}

func (d DefaultQueryPlanner) CreatePhyExpr(e logicalplan.Expr, schema containers.ISchema) (expr_eval.Expr, error) {
	switch v := e.(type) {
	case logicalplan.Column:
		return expr_eval.ColumnExpression{Index: schema.IndexOf(v.Name)}, nil
	case logicalplan.LiteralInt64:
		return expr_eval.LiteralInt64Expression{Value: v.Val}, nil
	case logicalplan.LiteralFloat64:
		return expr_eval.LiteralFloat64Expression{Value: v.Val}, nil
	case logicalplan.LiteralString:
		return expr_eval.LiteralStringExpression{Value: v.Val}, nil
	case logicalplan.BooleanBinaryExpr:
		l, err := d.CreatePhyExpr(v.L, schema)
		if err != nil {
			return nil, err
		}
		r, err := d.CreatePhyExpr(v.R, schema)
		if err != nil {
			return nil, err
		}
		return expr_eval.BooleanBinaryExpr{L: l, R: r, Op: v.Op}, nil
	default:
		return nil, errors.New("expr not implemented")
	}
}

func (d DefaultQueryPlanner) CreatePhyPlan(lp logicalplan.LogicalPlan, state ExecState) (operators.PhysicalPlan, error) {
	var visitErr error
	var source operators.PhysicalPlan
	var prev operators.PhysicalPlan
	lp.Accept(PostPlanVisitorFunc(func(plan logicalplan.LogicalPlan) bool {
		switch lPlan := plan.(type) {
		case logicalplan.Scan:
			scan := &operators.Input{Source: lPlan.Source, Projection: lPlan.Projection}
			source = scan
			prev = scan
		case logicalplan.Projection:
			projExpr := make([]expr_eval.Expr, len(lPlan.Proj))
			for i, e := range lPlan.Proj {
				schema := prev.Schema()
				projExpr[i], _ = d.CreatePhyExpr(e, schema)
			}
			projSchema := lPlan.Schema()

			projection := &operators.Projection{Proj: projExpr, Sch: projSchema}
			prev.SetNext(projection)
			prev = projection

		case logicalplan.Selection:
			schema := prev.Schema()
			filterExpr, _ := d.CreatePhyExpr(lPlan.Filter, schema)

			selection := &operators.Selection{Filter: filterExpr}
			prev.SetNext(selection)
			prev = selection

		case logicalplan.Out:
			callback := lPlan.Callback
			out := &operators.Output{OutputCallback: callback}
			prev.SetNext(out)
			prev = out
		default:
			visitErr = errors.New("plan not implemented")
		}
		return visitErr == nil
	}))

	return source, visitErr
}
