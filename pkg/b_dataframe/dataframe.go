package dataframe

import (
	"github.com/olekukonko/tablewriter"
	"os"
	logicalplan "tiny_planner/pkg/e_logical_plan"
	phyiscalplan "tiny_planner/pkg/f_physicalplan"
	"tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

type IDataFrame interface {
	Project(expr ...logicalplan.Expr) IDataFrame
	Filter(expr logicalplan.Expr) IDataFrame
	Aggregate(groupBy []logicalplan.Expr, aggregateExpr []logicalplan.AggregateExpr) IDataFrame

	Schema() (containers.ISchema, error)
	Collect() ([]containers.Batch, error)
	Show() error

	LogicalPlan() (logicalplan.LogicalPlan, error)
	PhysicalPlan() (phyiscalplan.PhysicalPlan, error)
}

type DataFrame struct {
	sessionState phyiscalplan.ExecState
	plan         logicalplan.LogicalPlan
}

func NewDataFrame(sessionState phyiscalplan.ExecState) *DataFrame {
	return &DataFrame{sessionState: sessionState}
}

func (df *DataFrame) Scan(path string, source datasource.TableReader, proj []string) IDataFrame {
	newPlan, err := logicalplan.NewBuilder().Scan(path, source, proj).Build()
	if err != nil {
		return nil
	}
	return &DataFrame{plan: newPlan, sessionState: df.sessionState}
}

func (df *DataFrame) Project(proj ...logicalplan.Expr) IDataFrame {
	newPlan, err := logicalplan.From(df.plan).Project(proj...).Build()
	if err != nil {
		return nil
	}
	return &DataFrame{plan: newPlan, sessionState: df.sessionState}
}

func (df *DataFrame) Filter(predicate logicalplan.Expr) IDataFrame {
	newPlan, err := logicalplan.From(df.plan).Filter(predicate).Build()
	if err != nil {
		return nil
	}
	return &DataFrame{plan: newPlan, sessionState: df.sessionState}
}

func (df *DataFrame) Aggregate(groupBy []logicalplan.Expr, aggExpr []logicalplan.AggregateExpr) IDataFrame {
	newPlan, err := logicalplan.From(df.plan).Aggregate(groupBy, aggExpr).Build()
	if err != nil {
		return nil
	}
	return &DataFrame{plan: newPlan, sessionState: df.sessionState}
}

func (df *DataFrame) TaskContext() execution.TaskContext {
	return df.sessionState.TaskContext()
}

func (df *DataFrame) Schema() (containers.ISchema, error) {
	return df.plan.Schema()
}

func (df *DataFrame) LogicalPlan() (logicalplan.LogicalPlan, error) {
	return df.plan, nil
}

func (df *DataFrame) Collect() ([]containers.Batch, error) {
	physicalPlan, err := df.PhysicalPlan()
	if err != nil {
		return nil, err
	}
	return physicalPlan.Execute(df.TaskContext())
}

func (df *DataFrame) Show() error {
	result, err := df.Collect()
	if err != nil {
		return err
	}
	table := tablewriter.NewWriter(os.Stdout)

	// 1. add headers
	headers := make([]string, 0)
	schema, err := df.Schema()
	if err != nil {
		return err
	}
	for _, field := range schema.Fields() {
		headers = append(headers, field.Name)
	}
	table.SetHeader(headers)

	// 2. add data
	for _, batch := range result {
		table.AppendBulk(batch.StringTable())
	}

	// 3. render
	table.Render()
	return nil
}

func (df *DataFrame) PhysicalPlan() (phyiscalplan.PhysicalPlan, error) {
	return df.sessionState.CreatePhysicalPlan(df.plan)
}
