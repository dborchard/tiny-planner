package dataframe

import (
	"github.com/olekukonko/tablewriter"
	"os"
	exprLogi "tiny_planner/pkg/e_logical_plan"
	exec "tiny_planner/pkg/f_exec_engine"
	"tiny_planner/pkg/f_exec_engine/a_operators"
	"tiny_planner/pkg/g_exec_runtime"
	containers "tiny_planner/pkg/i_containers"
)

type IDataFrame interface {
	Project(expr []exprLogi.LogicalExpr) IDataFrame
	Filter(expr exprLogi.LogicalExpr) IDataFrame
	Aggregate(groupBy []exprLogi.LogicalExpr, aggregateExpr []exprLogi.AggregateExpr) IDataFrame

	Schema() containers.Schema
	Collect() []containers.Batch
	Show()

	LogicalPlan() exprLogi.LogicalPlan
	PhysicalPlan() exprPhy.ExecutionPlan
}

type DataFrame struct {
	sessionState exec.ExecState
	plan         exprLogi.LogicalPlan
}

func NewDataFrame(sessionState exec.ExecState, plan exprLogi.LogicalPlan) *DataFrame {
	return &DataFrame{sessionState: sessionState, plan: plan}
}

func (df *DataFrame) Project(proj []exprLogi.LogicalExpr) IDataFrame {
	newPlan := exprLogi.From(df.plan).Project(proj).Build()
	return &DataFrame{plan: newPlan, sessionState: df.sessionState}
}

func (df *DataFrame) Filter(predicate exprLogi.LogicalExpr) IDataFrame {
	newPlan := exprLogi.From(df.plan).Filter(predicate).Build()
	return &DataFrame{plan: newPlan, sessionState: df.sessionState}
}

func (df *DataFrame) Aggregate(groupBy []exprLogi.LogicalExpr, aggExpr []exprLogi.AggregateExpr) IDataFrame {
	newPlan := exprLogi.From(df.plan).Aggregate(groupBy, aggExpr).Build()
	return &DataFrame{plan: newPlan, sessionState: df.sessionState}
}

func (df *DataFrame) TaskContext() execution.TaskContext {
	return df.sessionState.TaskContext()
}

func (df *DataFrame) Schema() containers.Schema {
	return df.plan.Schema()
}

func (df *DataFrame) LogicalPlan() exprLogi.LogicalPlan {
	return df.plan
}

func (df *DataFrame) Collect() []containers.Batch {
	physicalPlan := df.PhysicalPlan()
	res := physicalPlan.Execute(df.TaskContext())
	return res
}

func (df *DataFrame) Show() {
	result := df.Collect()
	table := tablewriter.NewWriter(os.Stdout)

	// 1. add headers
	headers := make([]string, 0)
	for _, field := range df.Schema().Fields() {
		headers = append(headers, field.Name)
	}
	table.SetHeader(headers)

	// 2. add data
	for _, batch := range result {
		table.AppendBulk(batch.StringTable())
	}

	// 3. render
	table.Render()
}

func (df *DataFrame) PhysicalPlan() exprPhy.ExecutionPlan {
	return df.sessionState.CreatePhysicalPlan(df.plan)
}
