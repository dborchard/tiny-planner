package catalog

import (
	"github.com/blastrain/vitess-sqlparser/tidbparser/dependency/model"
	types "tiny_planner/pkg/a_containers/a_types"
)

type ColDef struct {
	Name string
	Type types.Type
	Idx  int
}

type TableDef struct {
	Name    string
	ColDefs []*ColDef
}

func (s *TableDef) AppendCol(name string, typ types.Type) error {
	def := &ColDef{
		Name: name,
		Type: typ,
	}
	def.Idx = len(s.ColDefs)
	s.ColDefs = append(s.ColDefs, def)
	return nil
}

func (s *TableDef) TableByName(dbName model.CIStr, tableName model.CIStr) (TableDef, error) {
	return *MockTableDef(tableName.String(), 2), nil
}

func NewTableDef(tableName string, colDefs []*ColDef) *TableDef {
	return &TableDef{
		Name:    tableName,
		ColDefs: colDefs,
	}
}
