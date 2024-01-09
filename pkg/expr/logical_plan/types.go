package logical_plan

type LogicalPlan interface {
}

type Projection struct{}
type Filter struct{}
type Window struct{}
