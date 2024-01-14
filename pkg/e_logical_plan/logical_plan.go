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
}

var _ LogicalPlan = Scan{}
var _ LogicalPlan = Selection{}
var _ LogicalPlan = Projection{}
var _ LogicalPlan = Aggregate{}

// ----------- Scan -------------

type Scan struct {
	Path       string
	Source     datasource.TableReader
	Projection []string
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
	Input LogicalPlan
	Proj  []Expr
}

func (p Projection) Schema() (containers.ISchema, error) {
	var fields []arrow.Field
	for _, e := range p.Proj {
		used, err := e.ColumnsUsed(p.Input)
		if err != nil {
			return nil, err
		}
		fields = append(fields, used...)
	}
	return containers.NewSchema(fields, nil), nil
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Input}
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
	Input  LogicalPlan
	Filter Expr
}

func (s Selection) Schema() (containers.ISchema, error) {
	return s.Input.Schema()
}

func (s Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.Input}
}

func (s Selection) String() string {
	return fmt.Sprintf("Filter: %s", s.Filter.String())
}

// ----------- Agg -------------

type Aggregate struct {
	Input         LogicalPlan
	GroupExpr     []Expr
	AggregateExpr []AggregateExpr
}

func (a Aggregate) Schema() (containers.ISchema, error) {
	var fields []arrow.Field
	for _, e := range a.GroupExpr {
		used, err := e.ColumnsUsed(a.Input)
		if err != nil {
			return nil, err
		}
		fields = append(fields, used...)
	}
	for _, e := range a.AggregateExpr {
		used, err := e.ColumnsUsed(a.Input)
		if err != nil {
			return nil, err
		}
		fields = append(fields, used...)
	}
	return containers.NewSchema(fields, nil), nil
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Input}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%s, aggregateExpr=%s", a.GroupExpr, a.AggregateExpr)
}
