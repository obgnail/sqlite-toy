package sqlite

import (
	"fmt"
	"github.com/pingcap/errors"
)

type DB struct {
	Tables map[string]*Table
}

func NewDB() *DB {
	return &DB{Tables: make(map[string]*Table)}
}

func (db *DB) AddTable(table *Table) {
	db.Tables[table.Name] = table
}

func (db *DB) GetTable(tableName string) *Table {
	return db.Tables[tableName]
}

func (db *DB) Query(sql string) error {
	return nil
}

func (db *DB) Exec(sql string) error {
	parser := &Parser{}
	Type := parser.GetSQLType(sql)
	switch Type {
	case INSERT:
		return db.Insert(parser, sql)
	case UPDATE:
	case DELETE:
	case SELECT:
	default:
	}
	return fmt.Errorf("ddddddd")
}

func (db *DB) Insert(parser *Parser, sql string) error {
	ast, err := parser.ParseInsert(sql)
	if err != nil {
		return errors.Trace(err)
	}

	table := db.GetTable(ast.Table)
	if table == nil {
		return fmt.Errorf("has no such table: %s", ast.Table)
	}

	constraintErr := table.CheckConstraint(ast)
	if constraintErr != nil {
		return fmt.Errorf("column %s. err: %s", constraintErr.Column, constraintErr.Err)
	}

	dataset := table.Format(ast)

	if err := NewPlan(table).Insert(dataset); err != nil {
		return errors.Trace(err)
	}

	return nil
}
