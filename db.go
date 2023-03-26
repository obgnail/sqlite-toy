package sqlite

import (
	"fmt"
	"github.com/pingcap/errors"
)

type DB struct {
	Tree        *BPTree
	RootPageIdx int
}

func NewDB() *DB {
	tree := NewBPTree(17, nil)

	return &DB{
		Tree:        tree,
		RootPageIdx: 0,
	}
}

func (db *DB) Query(sql string) error {
	return nil
}

func (db *DB) Insert(sql string) error {
	parser := &Parser{}
	if parser.GetSQLType(sql) != INSERT {
		return fmt.Errorf("not a INSERT statement")
	}
	ast, err := parser.ParseInsert(sql)
	if err != nil {
		return errors.Trace(err)
	}

	table := GetTable(ast.Table)
	constraintErr := table.CheckConstraint(ast)
	if constraintErr != nil {
		return fmt.Errorf("column %s. err: %s", constraintErr.Column, constraintErr.Err)
	}

	dataset := table.Format(ast)

	if err := NewPlan(db).Insert(dataset); err != nil {
		return errors.Trace(err)
	}

	return nil
}
