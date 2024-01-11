package exec

import (
	exprLogi "tiny_planner/pkg/e_logical_plan"
	"tiny_planner/pkg/f_exec_engine/a_operators"
	"tiny_planner/pkg/f_exec_engine/b_expr_eval"
	containers "tiny_planner/pkg/i_containers"
)

type QueryPlanner interface {
	CreatePhyExpr(e exprLogi.LogicalExpr, schema containers.Schema) exprExec.Expression
	CreatePhyPlan(lp exprLogi.LogicalPlan, state ExecState) exprPhy.ExecutionPlan
}

type DefaultQueryPlanner struct {
}

func (d DefaultQueryPlanner) CreatePhyExpr(e exprLogi.LogicalExpr, schema containers.Schema) exprExec.Expression {
	switch v := e.(type) {
	case exprLogi.Column:
		return exprExec.ColumnExpression{I: schema.IndexOf(v.Name)}
	case exprLogi.LiteralInt64:
		return exprExec.LiteralInt64Expression{Value: v.Val}
	case exprLogi.LiteralFloat64:
		return exprExec.LiteralFloat64Expression{Value: v.Val}
	case exprLogi.LiteralString:
		return exprExec.LiteralStringExpression{Value: v.Val}
	case exprLogi.BooleanBinaryExpr:
		panic("not implemented")
	default:
		panic("not implemented")
	}
}

func (d DefaultQueryPlanner) CreatePhyPlan(lp exprLogi.LogicalPlan, state ExecState) exprPhy.ExecutionPlan {
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

func (d DefaultQueryPlanner) createScan(scan exprLogi.Scan, state ExecState) exprPhy.ExecutionPlan {
	return exprPhy.ScanExec{Source: scan.Source, Projection: scan.Projection}
}

func (d DefaultQueryPlanner) createProjection(projection exprLogi.Projection, state ExecState) exprPhy.ExecutionPlan {
	input := d.CreatePhyPlan(projection.Input, state)

	proj := make([]exprExec.Expression, len(projection.Expr))
	for i, e := range projection.Expr {
		proj[i] = d.CreatePhyExpr(e, input.Schema())
	}

	return exprPhy.ProjectionExec{Input: input, Proj: proj, Sch: projection.Schema()}
}

func (d DefaultQueryPlanner) createSelection(selection exprLogi.Selection, state ExecState) exprPhy.ExecutionPlan {
	input := d.CreatePhyPlan(selection.Input, state)
	return exprPhy.SelectionExec{Input: input, Filter: d.CreatePhyExpr(selection.Filter, input.Schema())}
}

func (d DefaultQueryPlanner) createAggregate(aggregate exprLogi.Aggregate, state ExecState) exprPhy.ExecutionPlan {
	panic("not implemented")
}
