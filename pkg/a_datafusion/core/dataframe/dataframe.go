package dataframe

import (
	"fmt"
	"tiny_planner/pkg/a_datafusion/common"
	"tiny_planner/pkg/a_datafusion/core/execution/context"
	"tiny_planner/pkg/a_datafusion/exprLogi"
	"tiny_planner/pkg/a_datafusion/exprPhy"
)

type IDataFrame interface {
	Project(expr []exprLogi.LogicalExpr) IDataFrame
	Filter(expr exprLogi.LogicalExpr) IDataFrame
	Aggregate(groupBy []exprLogi.LogicalExpr, aggregateExpr []exprLogi.AggregateExpr) IDataFrame

	Schema() common.Schema
	Collect() []common.Batch
	Show()

	LogicalPlan() exprLogi.LogicalPlan
	PhysicalPlan() exprPhy.PhysicalPlan
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
	return &DataFrame{plan: newPlan}
}

func (df *DataFrame) Filter(predicate exprLogi.LogicalExpr) IDataFrame {
	newPlan := exprLogi.From(df.plan).Filter(predicate).Build()
	return &DataFrame{plan: newPlan}
}

func (df *DataFrame) Aggregate(groupBy []exprLogi.LogicalExpr, aggExpr []exprLogi.AggregateExpr) IDataFrame {
	newPlan := exprLogi.From(df.plan).Aggregate(groupBy, aggExpr).Build()
	return &DataFrame{plan: newPlan}
}

func (df *DataFrame) TaskContext() context.TaskContext {
	return df.sessionState.TaskContext()
}

func (df *DataFrame) Schema() common.Schema {
	return df.plan.Schema()
}

func (df *DataFrame) LogicalPlan() exprLogi.LogicalPlan {
	return df.plan
}

func (df *DataFrame) Collect() []common.Batch {
	//taskCtx := df.TaskContext()
	//physicalPlan := df.PhysicalPlan()
	//res, _ := physicalplan.Collect(taskCtx, physicalPlan)
	//return res
	return nil
}

func (df *DataFrame) Show() {
	result := df.Collect()
	for _, batch := range result {
		fmt.Println(batch)
	}
}

func (df *DataFrame) PhysicalPlan() exprPhy.PhysicalPlan {
	return df.sessionState.CreatePhysicalPlan(df.plan)
}
