package dataframe

//
//// ITableProvider is the interface representing the ITableProvider trait in Rust
//type ITableProvider interface {
//	GetLogicalPlan() *logical_plan.LogicalPlan
//	SupportsFilterPushdown(filter *Expr) (TableProviderFilterPushDown, error)
//	Schema() SchemaRef
//	TableType() TableType
//	Scan(ctx context.Context, state *SessionState, projection []int, filters []*Expr, limit *int) (*ExecutionPlan, error)
//}
//
//type TableProvider struct {
//	Plan logical_plan.LogicalPlan
//}
//
//func (d *TableProvider) GetLogicalPlan() *logical_plan.LogicalPlan {
//	return &d.Plan
//}
//
//func (d *TableProvider) SupportsFilterPushDown(filter *Expr) (TableProviderFilterPushDown, error) {
//	// Implement filter pushdown logic
//	return Exact, nil
//}
//
//// Schema returns the schema reference
//func (d *TableProvider) Schema() common.Schema {
//	d.Plan.Schema()
//	return &schema
//}
//
//// TableType returns the type of the table
//func (d *TableProvider) TableType() TableType {
//	return View
//}
//
//// Scan performs a scan with optional projection, filters, and limit
//func (d *TableProvider) Scan(ctx context.Context, state *SessionState, projection []int, filters []*Expr, limit *int) (*ExecutionPlan, error) {
//	// Implement scan logic with goroutines and channels for asynchronous behavior
//	return nil, nil
//}
