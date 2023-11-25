package plancore

import (
	"fmt"
	"github.com/blastrain/vitess-sqlparser/tidbparser/ast"
	types "tiny_planner/a_types"
	catalog "tiny_planner/b_catalog"
)

type Expr interface {
	fmt.Stringer
}

type exprImpl struct {
	Expr
}

func (node *exprImpl) String() string {
	return ""
}

var _ Expr = new(ExprCol)
var _ Expr = new(ExprFunc)
var _ Expr = new(ExprUnary)
var _ Expr = new(ExprBinary)

type ExprCol struct {
	exprImpl
	Type   types.Type
	ColIdx int
}

func (e *ExprCol) String() string {
	return fmt.Sprintf("(col %s)", e.Type.String())
}

type ExprFunc struct {
	exprImpl
	Type types.Type
	Name string
	Args []Expr
}

func (e *ExprFunc) String() string {
	return fmt.Sprintf("%s(%s)", e.Name, e.Args)
}

type ExprUnary struct {
	exprImpl
	op    string
	child Expr
}

func (e *ExprUnary) String() string {
	return fmt.Sprintf("%s(%s)", e.op, e.child)
}

type ExprBinary struct {
	exprImpl
	left  Expr
	op    string
	right Expr
}

func (e *ExprBinary) String() string {
	return fmt.Sprintf("(%s %s %s)", e.left, e.op, e.right)
}

func ColDefToExprCol(colDef []*catalog.ColDef) []ExprCol {
	var exprCols []ExprCol
	for _, col := range colDef {
		exprCols = append(exprCols, ExprCol{
			Type:   col.Type,
			ColIdx: col.Idx,
		})
	}
	return exprCols
}

func ArgsToExprs(args []ast.ExprNode) []Expr {
	var exprs []Expr
	for _, arg := range args {
		switch v := arg.(type) {
		case *ast.ColumnNameExpr:
			exprs = append(exprs, &ExprCol{
				Type:   types.T_int32.ToType(),
				ColIdx: 0,
			})
		case *ast.FuncCallExpr:
			exprs = append(exprs, &ExprFunc{
				Name: v.FnName.L,
				Args: ArgsToExprs(v.Args),
			})
		}
	}
	return exprs
}
