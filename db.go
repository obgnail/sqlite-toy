package sqlite

import (
	"fmt"
	"github.com/pingcap/errors"
	"strconv"
	"strings"
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

func (db *DB) Query(sql string) ([]*BPItem, error) {
	parser := &Parser{}
	Type := parser.GetSQLType(sql)
	if Type != SELECT {
		return nil, fmt.Errorf("is not select sql")
	}
	return db.query(parser, sql)
}

func (db *DB) Exec(sql string) error {
	parser := &Parser{}
	Type := parser.GetSQLType(sql)
	switch Type {
	case INSERT:
		return db.Insert(parser, sql)
	case UPDATE:
		return db.Update(parser, sql)
	case DELETE:
		return db.Delete(parser, sql)
	case CREATE:
		return db.CreateTable(parser, sql)
	default:
		return fmt.Errorf("unsuported sql")
	}
}

func (db *DB) CreateTable(parser *Parser, sql string) error {
	ast, err := parser.ParseCreateTable(sql)
	if err != nil {
		return errors.Trace(err)
	}
	table, err := db.NewTable(ast)
	if err != nil {
		return fmt.Errorf("new table err: %s", err)
	}
	db.AddTable(table)
	return nil
}

func (db *DB) NewTable(ast *CreateTableAST) (*Table, error) {
	table := &Table{
		Name:         ast.Table,
		PrimaryKey:   ast.PrimaryKey,
		Columns:      ast.Columns,
		Indies:       map[string]*BPTree{"-": NewBPTree(17, nil)},
		Formatter:    make(map[string]func(data string) interface{}, len(ast.Columns)),
		DefaultValue: make([]interface{}, 0, len(ast.Columns)),
		Constraint:   make(map[string]func(data string) error, len(ast.Columns)),
	}

	for idx, col := range ast.Columns {
		t := ast.Type[idx]
		if strings.HasPrefix(t, "INTEGER") {

			table.Formatter[col] = IntegerFormatter

			if ast.Default[idx] != "" {
				val, err := strconv.Atoi(ast.Default[idx])
				if err != nil {
					return nil, errors.Trace(err)
				}
				table.DefaultValue = append(table.DefaultValue, val)
			} else {
				table.DefaultValue = append(table.DefaultValue, 0)
			}

			if col == ast.PrimaryKey {
				table.Constraint[col] = Compose(IsInteger, NotEmpty)
			} else {
				table.Constraint[col] = IsInteger
			}

		} else if strings.HasPrefix(t, "VARCHAR") {
			table.Formatter[col] = StringFormatter

			_default := ast.Default[idx]
			_default = TrimQuotes(_default)
			table.DefaultValue = append(table.DefaultValue, _default)

			_type := t
			_type = strings.TrimLeft(_type, "VARCHAR")
			_type = strings.TrimLeft(_type, "(")
			_type = strings.TrimRight(_type, ")")
			length, err := strconv.Atoi(_type)
			if err != nil {
				return nil, errors.Trace(err)
			}
			table.Constraint[col] = func(data string) error { return VarcharTooLong(data, length) }
		}

	}

	return table, nil
}

func (db *DB) Delete(parser *Parser, sql string) error {
	ast, err := parser.ParseDelete(sql)
	if err != nil {
		return errors.Trace(err)
	}
	table := db.GetTable(ast.Table)
	if table == nil {
		return fmt.Errorf("has no such table: %s", ast.Table)
	}

	constraintErr := table.CheckDeleteConstraint(ast)
	if constraintErr != nil {
		return fmt.Errorf("column %s. err: %s", constraintErr.Column, constraintErr.Err)
	}

	if err := NewPlan(table).Delete(ast); err != nil {
		return errors.Trace(err)
	}
	return nil
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

func (db *DB) Update(parser *Parser, sql string) error {
	ast, err := parser.ParseUpdate(sql)
	if err != nil {
		return errors.Trace(err)
	}
	table := db.GetTable(ast.Table)
	if table == nil {
		return fmt.Errorf("has no such table: %s", ast.Table)
	}
	constraintErr := table.CheckUpdateConstraint(ast)
	if constraintErr != nil {
		return fmt.Errorf("column %s. err: %s", constraintErr.Column, constraintErr.Err)
	}

	if err := NewPlan(table).Update(ast); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (db *DB) query(parser *Parser, sql string) ([]*BPItem, error) {
	ast, err := parser.ParseSelect(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	table := db.GetTable(ast.Table)
	if table == nil {
		return nil, fmt.Errorf("has no such table: %s", ast.Table)
	}

	constraintErr := table.CheckSelectConstraint(ast)
	if constraintErr != nil {
		return nil, fmt.Errorf("column %s. err: %s", constraintErr.Column, constraintErr.Err)
	}

	plan := NewPlan(table)
	result, err := plan.Select(ast)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}
