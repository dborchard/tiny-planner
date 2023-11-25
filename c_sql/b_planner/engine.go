package planner

//
//func Optimize(ctx context.Context, stmt ast.Node, is *catalog.TableDef) (planphysical.PhysicalPlan, error) {
//	// 1. Build Logical Plan
//	builder := planlogical.NewPlanBuilder(context.TODO(), is)
//	logicPlan, err := builder.Build(ctx, stmt)
//	if err != nil {
//		return nil, err
//	}
//
//	// 2. Optimize Logical Plan
//	logicPlan, err = planlogical.Optimize(ctx, logicPlan.(planlogical.LogicalPlan))
//	if err != nil {
//		return nil, err
//	}
//
//	// 3. Optimize Physical Plan & 4. Build Physical Plan
//	phyPlan, err := planphysical.Optimize(ctx, logicPlan.(planlogical.LogicalPlan))
//	if err != nil {
//		return nil, err
//	}
//
//	return phyPlan, nil
//}
