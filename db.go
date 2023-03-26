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

func (db *DB) Exec(sql string) error {
	parser := &Parser{}
	Type := parser.GetSQLType(sql)
	switch Type {
	case SELECT:
		return db.Query(parser, sql)
	case INSERT:
		return db.Insert(parser, sql)
	case UPDATE:
	case DELETE:
	default:
	}
	return fmt.Errorf("exec error")
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

	constraintErr := table.CheckInsertConstraint(ast)
	if constraintErr != nil {
		return fmt.Errorf("column %s. err: %s", constraintErr.Column, constraintErr.Err)
	}

	dataset := table.Format(ast)

	if err := NewPlan(table).Insert(dataset); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (db *DB) Query(parser *Parser, sql string) error {
	ast, err := parser.ParseSelect(sql)
	if err != nil {
		return errors.Trace(err)
	}

	table := db.GetTable(ast.Table)
	if table == nil {
		return fmt.Errorf("has no such table: %s", ast.Table)
	}

	constraintErr := table.CheckSelectConstraint(ast)
	if constraintErr != nil {
		return fmt.Errorf("column %s. err: %s", constraintErr.Column, constraintErr.Err)
	}

	if err := NewPlan(table).Select(ast); err != nil {
		return errors.Trace(err)
	}

	return nil
}
