package phyiscial_plan

import (
	"context"
	"fmt"
	"tiny_planner/pkg/core/arrow_array"
	"tiny_planner/pkg/execution"
)

type ExecutionPlan interface {
}

// Collect executes the ExecutionPlan and collects the results in memory.
func Collect(taskContext execution.TaskContext, plan ExecutionPlan) ([]arrow_array.RecordBatch, error) {
	stream, err := ExecuteStream(plan, &taskContext)
	if err != nil {
		return nil, fmt.Errorf("failed to execute stream: %w", err)
	}

	return CommonCollect(ctx, stream)
}

// ExecuteStream starts the execution of the plan and returns a stream of results.
// You need to define the return type and implementation based on your application's architecture.
func ExecuteStream(plan ExecutionPlan, taskContext *TaskContext) (<-chan RecordBatch, error) {
	// Implementation of this function
	// This should start executing the plan and send results to a channel.
}

// CommonCollect collects results from the stream and returns them.
// This function assumes that the results are sent through a channel.
func CommonCollect(ctx context.Context, stream <-chan RecordBatch) ([]RecordBatch, error) {
	var results []RecordBatch
	for {
		select {
		case batch, ok := <-stream:
			if !ok {
				return results, nil // Stream closed, return collected results
			}
			results = append(results, batch)

		case <-ctx.Done():
			return nil, ctx.Err() // Context cancelled or expired
		}
	}
}
