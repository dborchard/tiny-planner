package logicalplan

import (
	"fmt"
	arrow2 "tiny_planner/pkg/core/arrow_array"
	"tiny_planner/pkg/core/common"
)

type Scan struct {
	Path       string
	Source     arrow2.DataSource
	Projection []string
}

func (s Scan) Schema() common.DFSchema {
	schema := s.Source.GetSchema()
	if len(s.Projection) == 0 {
		return schema
	} else {
		return schema.Select(s.Projection)
	}
}

func (s Scan) Children() []LogicalPlan {
	return []LogicalPlan{}
}

func (s Scan) String() string {
	if len(s.Projection) == 0 {
		return fmt.Sprintf("Scan: %s; projection=None", s.Path)
	}
	return fmt.Sprintf("Scan: %s; projection=%v", s.Path, s.Projection)
}
