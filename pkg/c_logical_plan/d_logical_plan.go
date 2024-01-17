package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strings"
	containers "tiny_planner/pkg/g_containers"
)

type LogicalPlan interface {
	Schema() containers.ISchema
	String() string

	//TODO: later on make this Children() and Accept() part of abstract class.

	Children() []LogicalPlan
	Accept(visitor PlanVisitor) bool
}

type PlanVisitor interface {
	PreVisit(plan LogicalPlan) bool
	PostVisit(plan LogicalPlan) bool
}

var _ LogicalPlan = Input{}
var _ LogicalPlan = Output{}

var _ LogicalPlan = Selection{}
var _ LogicalPlan = Projection{}
var _ LogicalPlan = Aggregate{}

// ----------- Projection -------------

type Projection struct {
	Next LogicalPlan
	Proj []Expr
}

func (p Projection) Accept(visitor PlanVisitor) bool {
	kontinue := visitor.PreVisit(p)
	if !kontinue {
		return false
	}

	if p.Children() != nil {
		// TODO: we should iterate
		kontinue = p.Children()[0].Accept(visitor)
		if !kontinue {
			return false
		}
	}

	return visitor.PostVisit(p)
}

func (p Projection) Schema() containers.ISchema {
	var fields []arrow.Field
	for _, e := range p.Proj {
		used := e.ColumnsUsed(p.Next)
		fields = append(fields, used...)
	}
	return containers.NewSchema(fields, nil)
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Next}
}

func (p Projection) String() string {
	var strList []string
	for _, e := range p.Proj {
		strList = append(strList, e.String())
	}
	s := strings.Join(strList, ", ")
	return fmt.Sprintf("Projection: %s", s)
}

// ----------- Selection -------------

type Selection struct {
	Next   LogicalPlan
	Filter Expr
}

func (s Selection) Accept(visitor PlanVisitor) bool {
	kontinue := visitor.PreVisit(s)
	if !kontinue {
		return false
	}

	if s.Children() != nil {
		// TODO: we should iterate
		kontinue = s.Children()[0].Accept(visitor)
		if !kontinue {
			return false
		}
	}

	return visitor.PostVisit(s)
}

func (s Selection) Schema() containers.ISchema {
	return s.Next.Schema()
}

func (s Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.Next}
}

func (s Selection) String() string {
	return fmt.Sprintf("Filter: %s", s.Filter.String())
}

// ----------- Agg -------------

type Aggregate struct {
	Next          LogicalPlan
	GroupExpr     []Expr
	AggregateExpr []AggregateExpr
}

func (a Aggregate) Accept(visitor PlanVisitor) bool {
	kontinue := visitor.PreVisit(a)
	if !kontinue {
		return false
	}

	if a.Children() != nil {
		// TODO: we should iterate
		kontinue = a.Children()[0].Accept(visitor)
		if !kontinue {
			return false
		}
	}

	return visitor.PostVisit(a)
}

func (a Aggregate) Schema() containers.ISchema {
	var fields []arrow.Field
	for _, e := range a.GroupExpr {
		used := e.ColumnsUsed(a.Next)
		fields = append(fields, used...)
	}
	for _, e := range a.AggregateExpr {
		used := e.ColumnsUsed(a.Next)
		fields = append(fields, used...)
	}
	return containers.NewSchema(fields, nil)
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Next}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%s, aggregateExpr=%s", a.GroupExpr, a.AggregateExpr)
}
