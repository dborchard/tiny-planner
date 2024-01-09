package context

import (
	"tiny_planner/a/engine"
	"tiny_planner/pkg/core/catalog"
	"tiny_planner/pkg/core/datasource"
	"tiny_planner/pkg/core/physical_optimizer"
	"tiny_planner/pkg/expr/logical_plan"
	"tiny_planner/pkg/optimizer"
	"tiny_planner/pkg/phyiscial_plan"
)

type SessionContext struct {
	SessionID string
	State     SessionState
}

func (c *SessionContext) RegisterCsv(name string, tablePath string, options datasource.CsvReadOptions) {

}

func (c *SessionContext) Sql(sql string) engine.DataFrame {
	return nil
}

type SessionState struct {
	SessionID string

	Optimizer optimizer.Optimizer

	PhysicalOptimizer physical_optimizer.PhysicalOptimizer

	QueryPlanner QueryPlanner

	CatalogList catalog.CatalogList

	TableFunctions map[string]datasource.TableFunction

	ScalarFunctions map[string]logical_plan.ScalarUDF

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
type QueryPlanner interface {
	CreatePhysicalPlan(lp logical_plan.LogicalPlan, state SessionState) phyiscial_plan.ExecutionPlan
}

func New() *SessionContext {
	return &SessionContext{}
}
