package context

import (
	"tiny_planner/pkg/a_datafusion/core/dataframe"
	logical_plan2 "tiny_planner/pkg/a_datafusion/expr/logicalplan"
	"tiny_planner/pkg/core/catalog"
	"tiny_planner/pkg/core/datasource"
	"tiny_planner/pkg/core/physical_optimizer"
	"tiny_planner/pkg/execution"
	"tiny_planner/pkg/optimizer"
	"tiny_planner/pkg/phyiscial_plan"
)

type SessionContext struct {
	SessionID string
	State     SessionState
}

func (c *SessionContext) RegisterCsv(name string, tablePath string, options datasource.CsvReadOptions) {

}

func (c *SessionContext) Sql(sql string) dataframe.IDataFrame {
	return nil
}

type SessionState struct {
	SessionID string

	Optimizer optimizer.Optimizer

	PhysicalOptimizer physical_optimizer.PhysicalOptimizer

	QueryPlanner QueryPlanner

	CatalogList catalog.CatalogList

	TableFunctions map[string]datasource.TableFunction

	ScalarFunctions map[string]logical_plan2.ScalarUDF

	//AggFunctions map[string]AggregateUDF
	//
	//WindowFunctions map[string]WindowUDF
	//
	//Config SessionConfig
	//
	//ExecutionProps ExecutionProps
	//
	//TableFactories map[string]TableProviderFactory
	//
	//RuntimeEnv RuntimeEnv
}

func (s SessionState) CreatePhysicalPlan(plan logical_plan2.LogicalPlan) phyiscial_plan.ExecutionPlan {
	return s.QueryPlanner.CreatePhysicalPlan(plan, s)
}

func (s SessionState) TaskContext() execution.TaskContext {
	return execution.NewTaskContext(s)
}

type QueryPlanner interface {
	CreatePhysicalPlan(lp logical_plan2.LogicalPlan, state SessionState) phyiscial_plan.ExecutionPlan
}

func New() *SessionContext {
	return &SessionContext{}
}
