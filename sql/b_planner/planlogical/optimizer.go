package planlogical

import (
	"context"
)

type logicalOptRule interface {
	Optimize(context.Context, LogicalPlan) (LogicalPlan, error)
	Name() string
}

var OptRuleList = []logicalOptRule{
	&columnPruner{},
}

func Optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {

	var err error
	for _, optimizer := range OptRuleList {
		lp, err = optimizer.Optimize(ctx, lp)
	}

	if err != nil {
		return nil, err
	}
	return lp, err
}
