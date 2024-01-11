## Tiny Query Engine

Porting `DataFusion` from `Rust` to `Golang` by referring to [drogo](https://github.com/briansterle/drogo).

### Modules

- [X] Datafusion/Core/DataFrame
- [X] Datafusion/Expr/LogicalPlan

### LogicalPlan vs PhysicalPlan

> One reason to keep logical and physical plans separate is that sometimes there can be multiple ways
> to execute a particular operation, meaning that there is a one-to-many relationship between logical
> plans and physical plans.
> 
> For example, there could be separate physical plans for single-process versus distributed execution,
> or CPU versus GPU execution.
> Logical Plan describes what you want. Physical Plan describes how you want to do it.
> In Physical Plan, you can have multiple ways to actually do it.