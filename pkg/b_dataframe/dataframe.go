package dataframe

import (
	"context"
	"github.com/olekukonko/tablewriter"
	"os"
	logicalplan "tiny_planner/pkg/e_logical_plan"
	phyiscalplan "tiny_planner/pkg/f_physicalplan"
	"tiny_planner/pkg/f_physicalplan/operators"
	"tiny_planner/pkg/g_exec_runtime"
	datasource "tiny_planner/pkg/h_storage_engine"
	containers "tiny_planner/pkg/i_containers"
)

type IDataFrame interface {
	Scan(path string, source datasource.TableReader, proj []string) IDataFrame
	Project(expr ...logicalplan.Expr) IDataFrame
	Filter(expr logicalplan.Expr) IDataFrame
	Aggregate(groupBy []logicalplan.Expr, aggregateExpr []logicalplan.AggregateExpr) IDataFrame

	Schema() containers.ISchema
	Collect(ctx context.Context, callback datasource.Callback) error
	Show() error

	LogicalPlan() (logicalplan.LogicalPlan, error)
	PhysicalPlan() (operators.PhysicalPlan, error)
}

type DataFrame struct {
	sessionState *phyiscalplan.ExecState
	planBuilder  *logicalplan.Builder
}

func NewDataFrame(sessionState *phyiscalplan.ExecState) IDataFrame {
	return &DataFrame{sessionState: sessionState}
}

func (df *DataFrame) Scan(path string, source datasource.TableReader, proj []string) IDataFrame {
	df.planBuilder = logicalplan.NewBuilder().Input(path, source, proj)
	return df
}

func (df *DataFrame) Project(proj ...logicalplan.Expr) IDataFrame {
	//TODO: Fix builder pattern
	df.planBuilder = df.planBuilder.Project(proj...)
	return df
}

func (df *DataFrame) Filter(predicate logicalplan.Expr) IDataFrame {
	df.planBuilder = df.planBuilder.Filter(predicate)
	return df
}

func (df *DataFrame) Aggregate(groupBy []logicalplan.Expr, aggExpr []logicalplan.AggregateExpr) IDataFrame {
	df.planBuilder = df.planBuilder.Aggregate(groupBy, aggExpr)
	return df
}

func (df *DataFrame) Collect(ctx context.Context, callback datasource.Callback) error {
	df.planBuilder = df.planBuilder.Output(callback)

	physicalPlan, err := df.PhysicalPlan()
	if err != nil {
		return err
	}
	return physicalPlan.Execute(df.TaskContext(), callback)
}

func (df *DataFrame) TaskContext() execution.TaskContext {
	return df.sessionState.TaskContext()
}

func (df *DataFrame) Schema() containers.ISchema {
	build, err := df.planBuilder.Build()
	if err != nil {
		panic(err)
	}
	return build.Schema()
}

func (df *DataFrame) LogicalPlan() (logicalplan.LogicalPlan, error) {
	return df.planBuilder.Build()
}

func (df *DataFrame) Show() error {

	result := make([]containers.IBatch, 0)
	err := df.Collect(context.TODO(), func(ctx context.Context, r containers.IBatch) error {
		result = append(result, r)
		return nil
	})

	if err != nil {
		return err
	}
	table := tablewriter.NewWriter(os.Stdout)

	// 1. add headers
	headers := make([]string, 0)
	schema := df.Schema()
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

func (df *DataFrame) PhysicalPlan() (operators.PhysicalPlan, error) {
	plan, err := df.LogicalPlan()
	if err != nil {
		return nil, err
	}
	return df.sessionState.CreatePhysicalPlan(plan)
}
