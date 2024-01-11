package dataframe

import (
	"fmt"
	exprLogi "tiny_planner/pkg/f_logical_plan"
	exprPhy "tiny_planner/pkg/g_physical_plan"
	"tiny_planner/pkg/h_exec_runtime"
	containers "tiny_planner/pkg/j_containers"
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
	sessionState SessionState
	plan         exprLogi.LogicalPlan
}

func NewDataFrame(sessionState SessionState, plan exprLogi.LogicalPlan) *DataFrame {
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
	for _, batch := range result {
		fmt.Println(batch)
	}
}

func (df *DataFrame) PhysicalPlan() exprPhy.ExecutionPlan {
	return df.sessionState.CreatePhysicalPlan(df.plan)
}
