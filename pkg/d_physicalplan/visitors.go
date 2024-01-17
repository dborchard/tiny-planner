package physicalplan

import logicalplan "tiny_planner/pkg/c_logical_plan"

// Fun Fact:Remember the time when https://www.quora.com/Whats-the-logic-behind-Google-rejecting-Max-Howell-the-author-of-Homebrew-for-not-being-able-to-invert-a-binary-tree
// Well, this is a time when I realized the importance of leetcode tree problems, PreOrder, InOrder, PostOrder traversal, etc.
// The logical plan is constructed in a top-down manner, and the physical plan is constructed in a bottom-up manner.
// Because in pull based approach, we need to know the schema of the child node before we can construct the parent node.
// So, we need to traverse the logical plan in a bottom-up manner. We call Next() on the child node to get the schema of the child node.
// But in push based approach, we need to know the schema of the parent node before we can construct the child node.
// So, we need PostOrder traversal of the logical plan.

type PostPlanVisitorFunc func(plan logicalplan.LogicalPlan) bool

func (f PostPlanVisitorFunc) PreVisit(_ logicalplan.LogicalPlan) bool {
	return true
}

func (f PostPlanVisitorFunc) PostVisit(plan logicalplan.LogicalPlan) bool {
	return f(plan)
}
