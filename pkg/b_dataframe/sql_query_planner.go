package dataframe

import (
	"tiny_planner/pkg/b_parser/ast"
	"tiny_planner/pkg/e_logical_plan"
)

type QueryPlanner interface {
	CreateLogExpr(e ast.SqlExpr, input IDataFrame) exprLogi.LogicalExpr
	CreateDataFrame(lp ast.SqlExpr, tables map[string]IDataFrame) IDataFrame
}
