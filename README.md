## Tiny SQL Planner

Inspired from [TinySQL](https://github.com/talent-plan/tinysql/tree/course/planner), [Velox](https://youtu.be/T9NMWN7vuSc?si=hCp8fGoSpSHnlBzr&t=51) and [MatrixOrigin](https://github.com/matrixorigin/matrixone/tree/main/pkg/sql).

### Main Components
- Logical Optimizer (Rule-Based) [here](pkg/c_sql/b_planner/planlogical/b_builder_test.go). Right now it only supports `Column Pruner`.

### Design
![Data Engine](docs/imgs/data_engine.png)


### TODO
- [x] Implement Parser from `TinySQL`
- [x] Implement Logical Plan Builder from `TinySQL`
- [x] Implement Logical Plan `Rule Based` Optimizer from `TinySQL`
- [x] Implement Operators from `MatrixOrigin` and `CockroachDB`
- [x] Implement Expression Evaluation from `MatrixOrigin`
- [x] Implement Expression Builder from `TinySQL`
- [ ] Implement Execution Pipeline,Stage from `LinDB`
- [ ] Implement Physical Plan Builder and Covert to Executors
- [ ] Implement Physical Plan `Cascade` Optimizer

### `Planner` Reference Projects
- [PingCAP-TinySQL](https://github.com/talent-plan/tinysql/tree/course/planner)
- [MatrixOrigin](https://github.com/matrixorigin/matrixone/tree/main/pkg/sql/plan)
- [polarsignals-FrostDB](https://github.com/polarsignals/frostdb/blob/c9100f2ac9c7aca111e1362be4a8a67b85b6b44b/query/logicalplan/optimize.go#L11)
- [RadonDB](https://github.com/radondb/radon/blob/master/src/optimizer/simple_optimizer.go)
- [Zalopay-ZPD](https://medium.com/zalopay-engineering/sql-planning-parser-optimizer-ee118a9705ed)
- [XiaoMi-Soar](https://github.com/XiaoMi/soar/blob/fab04633b12ba1e4f35456112360150a6d0d1421/advisor/rules.go#L119)
- [CockroachDB](https://github.com/cockroachdb/cockroach/blob/c097a16427f65e9070991f062716d222ea5903fe/pkg/sql/opt/doc.go#L12)
- [LinDB](https://github.com/lindb/lindb/tree/main/query)
- [cashapp-PranaDB](https://github.com/cashapp/pranadb/tree/main/tidb)
- [Youtube-vitess](https://github.com/vitessio/vitess/blob/3404baa17b47be5565fe48d0003ae63c3037411c/go/vt/vttablet/tabletmanager/vdiff/table_plan.go#L67)

### `Execution Engine` Reference Projects
- [TinySQL](https://github.com/talent-plan/tinysql/blob/4ec59ec661086305be82f885768490706a4c4723/expression/builtin.go#L332)
- [PranaDB](https://github.com/cashapp/pranadb/blob/b0d98ad1c632b88da65ad2bf0d4ecb68be89df5e/tidb/expression/builtin.go#L524)
- [MatrixOrigin](https://github.com/matrixorigin/matrixone/blob/67141f025433e32fe0343fba1035e9232fb20f11/pkg/sql/plan/function/function.go#L34)
- [CockroachDB](https://github.com/cockroachdb/cockroach/blob/01e65172dcb17384db33e8229d16461f6f99c01d/pkg/sql/sem/builtins/builtinsregistry/builtins_registry.go#L21)
- [Vitess](https://github.com/vitessio/vitess/blob/faf9815b56a7d0d46903cab1d3730c8bd0ba618a/go/vt/vtgate/evalengine/translate_builtin.go#L64)
- [Facebook-velox](https://www.youtube.com/watch?v=T9NMWN7vuSc&t=45s)
- [MatrixOrigin](https://github.com/matrixorigin/matrixone/tree/main/pkg/sql/plan/function)
