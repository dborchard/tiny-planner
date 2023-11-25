## Tiny SQL Planner

Inspired from [TinySQL](https://github.com/talent-plan/tinysql/tree/course/planner).

### Main Components
- Logical Optimizer (Rule Based) [here](c_sql/b_planner/planlogical/b_builder_test.go). Right now it only supports `Column Pruner`.

### TODO
- [x] Implement Parser
- [x] Implement Logical Plan Builder
- [x] Implement Logical Plan `Rule Based` Optimizer
- [ ] Implement Operators
- [ ] Implement Expression Evaluation
- [ ] Implement Physical Plan Builder 
- [ ] Implement Physical Plan `Cascade` Optimizer