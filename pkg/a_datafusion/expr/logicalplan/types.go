package logicalplan

import (
	"tiny_planner/pkg/core/common"
)

type LogicalPlan interface {
	Schema() common.DFSchema

	Children() []LogicalPlan
	String() string
}

var _ LogicalPlan = Scan{}
var _ LogicalPlan = Selection{}
var _ LogicalPlan = Projection{}
var _ LogicalPlan = Aggregate{}
