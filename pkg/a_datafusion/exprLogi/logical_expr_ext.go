package exprLogi

// ------------- Agg -------------

func Sum(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"SUM", input}
}

func Min(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MIN", input}
}

func Max(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"MAX", input}
}

func Avg(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"AVG", input}
}

func Count(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"COUNT", input}
}

// ------------- BinaryExpr -------------

func Eq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"eq", "=", l, r}
}
func Neq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"neq", "!=", l, r}
}
func Gt(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gt", ">", l, r}
}
func GtEq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"gteq", ">=", l, r}
}
func Lt(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lt", "<", l, r}
}
func LtEq(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"lteq", "<=", l, r}
}
func And(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"and", "AND", l, r}
}
func Or(l LogicalExpr, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{"or", "OR", l, r}
}

// ------------- MathExpr -------------

func Add(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"add", "+", l, r}
}

func Subtract(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"subtract", "-", l, r}
}

func Multiply(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"multiply", "*", l, r}
}

func Divide(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"divide", "/", l, r}
}

func Modulus(l LogicalExpr, r LogicalExpr) MathExpr {
	return MathExpr{"modulus", "%", l, r}
}
