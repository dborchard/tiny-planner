package exprLogi

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strings"
	datasource "tiny_planner/pkg/j_storage_engine"
	containers "tiny_planner/pkg/k_containers"
)

type LogicalPlan interface {
	Schema() containers.Schema
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
	Source     datasource.DataSource
	Projection []string
}

func (s Scan) Schema() containers.Schema {
	schema := s.Source.Schema()
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
		return fmt.Sprintf("Scan: %s; projExpr=None", s.Path)
	}
	return fmt.Sprintf("Scan: %s; projExpr=%v", s.Path, s.Projection)
}

// ----------- Projection -------------

type Projection struct {
	Input LogicalPlan
	Expr  []LogicalExpr
}

func (p Projection) Schema() containers.Schema {
	var fields []arrow.Field
	for _, e := range p.Expr {
		fields = append(fields, e.ToField(p.Input))
	}
	return containers.Schema{Schema: arrow.NewSchema(fields, nil)}
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Input}
}

func (p Projection) String() string {
	var strList []string
	for _, e := range p.Expr {
		strList = append(strList, e.String())
	}
	s := strings.Join(strList, ", ")
	return fmt.Sprintf("Projection: %s", s)
}

// ----------- Selection -------------

type Selection struct {
	Input  LogicalPlan
	Filter LogicalExpr
}

func (s Selection) Schema() containers.Schema {
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
	GroupExpr     []LogicalExpr
	AggregateExpr []AggregateExpr
}

func (a Aggregate) Schema() containers.Schema {
	var fields []arrow.Field
	for _, e := range a.GroupExpr {
		fields = append(fields, e.ToField(a.Input))
	}
	for _, e := range a.AggregateExpr {
		fields = append(fields, e.ToField(a.Input))
	}
	return containers.Schema{Schema: arrow.NewSchema(fields, nil)}
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Input}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%s, aggregateExpr=%s", a.GroupExpr, a.AggregateExpr)
}
