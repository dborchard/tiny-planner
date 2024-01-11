package exprLogi

type Builder struct {
	plan LogicalPlan
}

func From(plan LogicalPlan) Builder {
	return Builder{plan: plan}
}

func (b Builder) Project(expr []LogicalExpr) Builder {
	projPlan := Projection{b.plan, expr}
	return Builder{plan: projPlan}
}

func (b Builder) Filter(pred LogicalExpr) Builder {
	selPlan := Selection{b.plan, pred}
	return Builder{plan: selPlan}
}

func (b Builder) Aggregate(groupBy []LogicalExpr, aggExpr []AggregateExpr) Builder {
	aggPlan := Aggregate{b.plan, groupBy, aggExpr}
	return Builder{plan: aggPlan}
}

func (b Builder) Build() LogicalPlan {
	return b.plan
}
