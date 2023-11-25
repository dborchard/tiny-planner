package tiny_planner

type DB struct {
}

func NewDB() *DB {
	return &DB{}
}

//func (db *DB) Process(sql string, callback func(ctx context.Context, r arrow.Record)) error {
//	//ctx := context.TODO()
//	//parsr := parser.New()
//	//stmt, err := parsr.ParseOneStmt(sql, "", "")
//
//	//infoSchema := catalog.GetInfoSchema(ctx)
//	//
//	//phyPlan, err := planner.Optimize(ctx, stmt, infoSchema)
//	//if err != nil {
//	//	return err
//	//}
//	//
//	//return phyPlan.Execute(ctx, callback)
//	panic("implement me")
//}
