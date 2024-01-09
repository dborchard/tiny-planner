package engine

import (
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"strings"
)

func Format(plan LogicalPlan, indent int) string {
	var sb strings.Builder
	for i := 0; i < indent; i++ {
		sb.WriteRune('\t')
	}
	sb.WriteString(plan.String())
	sb.WriteRune('\n')
	for _, child := range plan.Children() {
		sb.WriteString(Format(child, indent+1))
	}
	return sb.String()
}

type Scan struct {
	Path       string
	Source     DataSource
	Projection []string
}

func (s Scan) Schema() Schema {
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

type Projection struct {
	Input LogicalPlan
	Expr  []LogicalExpr
}

func (p Projection) Schema() Schema {
	fields := []arrow.Field{}
	for _, e := range p.Expr {
		fields = append(fields, e.ToField(p.Input))
	}
	return Schema{arrow.NewSchema(fields, nil)}
}

func (p Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Input}
}

func (p Projection) String() string {
	strs := []string{}
	for _, e := range p.Expr {
		strs = append(strs, e.String())
	}
	s := strings.Join(strs, ", ")
	return fmt.Sprintf("Projection: %s", s)
}

type Selection struct {
	Input LogicalPlan
	Expr  LogicalExpr
}

func (s Selection) Schema() Schema {
	return s.Input.Schema()
}

func (s Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.Input}
}

func (s Selection) String() string {
	return fmt.Sprintf("Filter: %s", s.Expr.String())
}

type Aggregate struct {
	Input         LogicalPlan
	GroupExpr     []LogicalExpr
	AggregateExpr []AggregateExpr
}

func (a Aggregate) Schema() Schema {
	fields := []arrow.Field{}
	for _, e := range a.GroupExpr {
		fields = append(fields, e.ToField(a.Input))
	}
	for _, e := range a.AggregateExpr {
		fields = append(fields, e.toField(a.Input))
	}
	return Schema{arrow.NewSchema(fields, nil)}
}

func (a Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Input}
}

func (a Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%s, aggregateExpr=%s", a.GroupExpr, a.AggregateExpr)
}
