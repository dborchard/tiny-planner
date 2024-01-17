package logicalplan

import (
	"fmt"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

// ----------- Input -------------

type Input struct {
	Path       string
	Source     datasource.TableReader
	Projection []string
}

func (s Input) Accept(visitor PlanVisitor) bool {
	kontinue := visitor.PreVisit(s)
	if !kontinue {
		return false
	}

	if len(s.Children()) > 0 {
		// TODO: we should iterate
		kontinue = s.Children()[0].Accept(visitor)
		if !kontinue {
			return false
		}
	}

	return visitor.PostVisit(s)
}

func (s Input) Schema() containers.ISchema {
	schema := s.Source.Schema()
	if len(s.Projection) == 0 {
		return schema
	} else {
		return schema.Select(s.Projection)
	}
}

func (s Input) Children() []LogicalPlan {
	return []LogicalPlan{}
}

func (s Input) String() string {
	if len(s.Projection) == 0 {
		return fmt.Sprintf("Input: %s; projExpr=None", s.Path)
	}
	return fmt.Sprintf("Input: %s; projExpr=%v", s.Path, s.Projection)
}

// ----------- Output -------------

type Output struct {
	Next     LogicalPlan
	Callback datasource.Callback
}

func (o Output) Schema() containers.ISchema {
	return o.Next.Schema()
}

func (o Output) Children() []LogicalPlan {
	return []LogicalPlan{o.Next}
}

func (o Output) String() string {
	return fmt.Sprintf("Output:")
}

func (o Output) Accept(visitor PlanVisitor) bool {
	kontinue := visitor.PreVisit(o)
	if !kontinue {
		return false
	}

	if len(o.Children()) > 0 {
		// TODO: we should iterate
		kontinue = o.Children()[0].Accept(visitor)
		if !kontinue {
			return false
		}
	}

	return visitor.PostVisit(o)
}
