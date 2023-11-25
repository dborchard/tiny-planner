package planlogical

import (
	"bytes"
	"context"
	"fmt"
	"github.com/blastrain/vitess-sqlparser/tidbparser/dependency/model"
	catalog "tiny_planner/b_catalog"
	"tiny_planner/sql/b_planner/plancore"
)

type LogicalPlan interface {
	plancore.Plan
	PredicatePushDown(predicates []plancore.Expr) LogicalPlan
	PruneColumns([]plancore.ExprCol) error

	SetChildren(...LogicalPlan)
	Children() []LogicalPlan
}

var _ LogicalPlan = &baseLogicalPlan{}
var _ LogicalPlan = &LogicalSelection{}
var _ LogicalPlan = &LogicalProjection{}
var _ LogicalPlan = &DataSource{}

type baseLogicalPlan struct {
	plancore.BasePlan
	self     LogicalPlan
	children []LogicalPlan
}

// Children implements LogicalPlan Children interface.
func (p *baseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

func (p *baseLogicalPlan) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// ExplainInfo implements Plan interface.
func (p *baseLogicalPlan) ExplainInfo() string {
	return ""
}

func newBaseLogicalPlan(ctx context.Context, self LogicalPlan) baseLogicalPlan {
	return baseLogicalPlan{
		BasePlan: plancore.NewBasePlan(ctx),
		self:     self,
		children: make([]LogicalPlan, 0),
	}
}

type LogicalSelection struct {
	baseLogicalPlan
	Conditions []plancore.Expr
}

func (p *LogicalSelection) Init(ctx context.Context) *LogicalSelection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, p)
	return p
}

func (p *LogicalSelection) ExplainInfo() string {
	buffer := bytes.NewBufferString("Selection: ")
	for _, cond := range p.Conditions {
		buffer.WriteString(cond.String())
	}

	child := ""
	for _, childPlan := range p.children {
		child += childPlan.ExplainInfo()
	}
	buffer.WriteString(child)

	return buffer.String()
}

type LogicalProjection struct {
	baseLogicalPlan
	Expressions []plancore.Expr
}

func (p *LogicalProjection) Init(ctx context.Context) *LogicalProjection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, p)
	return p
}

func (p *LogicalProjection) ExplainInfo() string {
	buffer := bytes.NewBufferString("Projection: ")
	for i, expr := range p.Expressions {
		buffer.WriteString(fmt.Sprintf("Column#%d", i))
		buffer.WriteString(expr.String())
		buffer.WriteString(", ")
	}
	child := ""
	for _, childPlan := range p.children {
		child += childPlan.ExplainInfo()
	}
	buffer.WriteString(child)

	return buffer.String()
}

type DataSource struct {
	baseLogicalPlan

	DBName   model.CIStr
	Columns  []plancore.ExprCol
	allConds []plancore.Expr
	table    catalog.TableDef
}

func (p *DataSource) Init(ctx context.Context) *DataSource {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, p)
	return p
}

func (p *DataSource) ExplainInfo() string {
	buffer := bytes.NewBufferString("DataSource: ")
	tblName := p.table.Name
	_, _ = fmt.Fprintf(buffer, "table:%s", tblName)

	child := ""
	for _, childPlan := range p.children {
		child += childPlan.ExplainInfo()
	}
	buffer.WriteString(child)

	return buffer.String()
}
