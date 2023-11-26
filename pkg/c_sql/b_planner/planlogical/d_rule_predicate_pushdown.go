package planlogical

import (
	"context"
	"tiny_planner/pkg/c_sql/c_exec_engine/c_expression_eval"
)

type ppdSolver struct{}

func (*ppdSolver) Name() string {
	return "predicate_push_down"
}

func (*ppdSolver) Optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	lp = lp.PredicatePushDown(nil)
	return lp, nil
}

func (p *baseLogicalPlan) PredicatePushDown(predicates []expression.Expr) LogicalPlan {
	if len(p.children) == 0 {
		return p.self
	}

	child := p.children[0]
	newChild := child.PredicatePushDown(predicates)
	addSelection(p.self, newChild, predicates, 0)
	return newChild
}

func addSelection(p LogicalPlan, child LogicalPlan, predicates []expression.Expr, chIdx int) {
	_selection := LogicalSelection{Conditions: predicates}
	selection := _selection.Init(p.SCtx())

	selection.SetChildren(child)
	p.Children()[chIdx] = selection
}

func (p *LogicalProjection) PredicatePushDown(predicates []expression.Expr) LogicalPlan {
	child := p.baseLogicalPlan.PredicatePushDown(predicates)
	return child
}

func (p *LogicalSelection) PredicatePushDown(predicates []expression.Expr) LogicalPlan {
	child := p.children[0]
	newChild := child.PredicatePushDown(predicates)
	p.Conditions = append(p.Conditions, predicates...)
	return newChild
}

func (p *DataSource) PredicatePushDown(predicates []expression.Expr) LogicalPlan {
	p.allConds = predicates
	return p
}
