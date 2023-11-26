package planlogical

import (
	"bytes"
	"context"
	"fmt"
	"github.com/blastrain/vitess-sqlparser/tidbparser/dependency/model"
	"tiny_planner/pkg/b_catalog"
	plancore2 "tiny_planner/pkg/c_sql/b_planner/plancore"
	"tiny_planner/pkg/c_sql/c_exec_engine/c_expression_eval"
)

type LogicalPlan interface {
	plancore2.Plan
	PredicatePushDown(predicates []expression.Expr) LogicalPlan
	PruneColumns([]expression.ExprCol) error

	SetChildren(...LogicalPlan)
	Children() []LogicalPlan
}

var _ LogicalPlan = &baseLogicalPlan{}
var _ LogicalPlan = &LogicalSelection{}
var _ LogicalPlan = &LogicalProjection{}
var _ LogicalPlan = &DataSource{}

type baseLogicalPlan struct {
	plancore2.BasePlan
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
		BasePlan: plancore2.NewBasePlan(ctx),
		self:     self,
		children: make([]LogicalPlan, 0),
	}
}

type LogicalSelection struct {
	baseLogicalPlan
	Conditions []expression.Expr
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
	Expressions []expression.Expr
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
	Columns  []expression.ExprCol
	allConds []expression.Expr
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
