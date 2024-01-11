package exprPhy

import (
	containers "tiny_planner/pkg/a_containers"
	exprLogi "tiny_planner/pkg/c_expr_logical"
)

func FromLogicalToPhysical(e exprLogi.LogicalExpr, schema containers.Schema) Expression {
	switch v := e.(type) {
	case exprLogi.Column:
		return ColumnExpression{i: schema.IndexOf(v.Name)}
	case exprLogi.LiteralInt64:
		return LiteralInt64Expression{value: v.Val}
	case exprLogi.LiteralFloat64:
		return LiteralFloat64Expression{value: v.Val}
	case exprLogi.LiteralString:
		return LiteralStringExpression{value: v.Val}
	default:
		panic("not implemented")
	}
}
