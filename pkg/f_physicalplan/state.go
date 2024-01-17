package physicalplan

import (
	"context"
	"time"
	logicalplan "tiny_planner/pkg/e_logical_plan"
	"tiny_planner/pkg/f_physicalplan/operators"
	"tiny_planner/pkg/g_exec_runtime"
)

type ExecState struct {
	SessionID        string
	SessionStartTime time.Time
	QueryPlanner     QueryPlanner
	RuntimeEnv       *execution.RuntimeEnv
}

func NewExecState(sessionId string) *ExecState {
	return &ExecState{
		SessionID:        sessionId,
		SessionStartTime: time.Now(),
		QueryPlanner:     DefaultQueryPlanner{},
		RuntimeEnv:       execution.NewRuntimeEnv(),
	}
}

func (s ExecState) TaskContext() execution.TaskContext {
	return execution.TaskContext{
		SessionID: s.SessionID,
		TaskID:    time.Now().String(),
		Runtime:   s.RuntimeEnv,
		Ctx:       context.Background(),
	}
}

func (s ExecState) CreatePhysicalPlan(plan logicalplan.LogicalPlan) (operators.PhysicalPlan, error) {
	return s.QueryPlanner.CreatePhyPlan(plan, s)
}
