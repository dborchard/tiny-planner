package logicalplan

import (
	"strings"
)

func PrettyPrint(plan LogicalPlan, indent int) string {
	var sb strings.Builder
	for i := 0; i < indent; i++ {
		sb.WriteRune('\t')
	}
	sb.WriteString(plan.String())
	sb.WriteRune('\n')
	for _, child := range plan.Children() {
		sb.WriteString(PrettyPrint(child, indent+1))
	}
	return sb.String()
}
