package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strings"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

type LogicalPlan interface {
	Schema() (containers.ISchema, error)
	Children() []LogicalPlan
	String() string
	Accept(visitor PlanVisitor) bool
}

type PlanVisitor interface {
	PreVisit(plan LogicalPlan) bool
	PostVisit(plan LogicalPlan) bool
}

var _ LogicalPlan = Scan{}
var _ LogicalPlan = Selection{}
var _ LogicalPlan = Projection{}
var _ LogicalPlan = Aggregate{}
var _ LogicalPlan = Out{}

// ----------- Scan -------------

type Scan struct {
	Path       string
	Source     datasource.TableReader
	Projection []string
}

func (s Scan) Accept(visitor PlanVisitor) bool {
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

func (s Scan) Schema() (containers.ISchema, error) {
	schema, err := s.Source.Schema()
	if err != nil {
		return nil, err
	}
	if len(s.Projection) == 0 {
		return schema, nil
	} else {
		return schema.Select(s.Projection)
	}
}

func (s Scan) Children() []LogicalPlan {
	return []LogicalPlan{}
}

func (s Scan) String() string {
	if len(s.Projection) == 0 {
		return fmt.Sprintf("Scan: %s; projExpr=None", s.Path)
	}
	return fmt.Sprintf("Scan: %s; projExpr=%v", s.Path, s.Projection)
}

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

func (p Projection) Schema() (containers.ISchema, error) {
	var fields []arrow.Field
	for _, e := range p.Proj {
		used, err := e.ColumnsUsed(p.Next)
		if err != nil {
			return nil, err
		}
		fields = append(fields, used...)
	}
	return containers.NewSchema(fields, nil), nil
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

func (s Selection) Schema() (containers.ISchema, error) {
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

func (a Aggregate) Schema() (containers.ISchema, error) {
	var fields []arrow.Field
	for _, e := range a.GroupExpr {
		used, err := e.ColumnsUsed(a.Next)
		if err != nil {
			return nil, err
		}
		fields = append(fields, used...)
	}
	for _, e := range a.AggregateExpr {
		used, err := e.ColumnsUsed(a.Next)
		if err != nil {
			return nil, err
		}
		fields = append(fields, used...)
	}
	return containers.NewSchema(fields, nil), nil
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Next}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%s, aggregateExpr=%s", a.GroupExpr, a.AggregateExpr)
}

// ----------- Out -------------

type Out struct {
	Next     LogicalPlan
	Callback datasource.Callback
}

func (o Out) Schema() (containers.ISchema, error) {
	return o.Next.Schema()
}

func (o Out) Children() []LogicalPlan {
	return []LogicalPlan{o.Next}
}

func (o Out) String() string {
	return fmt.Sprintf("Out:")
}

func (o Out) Accept(visitor PlanVisitor) bool {
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
