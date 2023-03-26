package sqlite

import "strings"

type Table struct {
	Name       string
	Columns    []string
	Constraint map[string]func(data string) error
	Formatter  map[string]func(data string) interface{}
	Indies     []map[string]int64 // map[columnName]int64, multi indies,maybe
}

var exampleTable = &Table{
	Name:    "user",
	Columns: []string{"id", "sex", "age", "username", "email", "phone"},
	Constraint: map[string]func(data string) error{
		"id":       Compose(IsInteger, NotEmpty),
		"sex":      func(data string) error { return OptionLimit(TrimQuotes(data), []string{"male", "female"}) },
		"age":      IsSignedInteger,
		"username": func(data string) error { return VarcharTooLong(data, 8) },
		"email":    IsString,
		"phone":    IsString,
	},
	Formatter: map[string]func(data string) interface{}{
		"id":       StringFormatter,
		"sex":      StringFormatter,
		"age":      integerFormatter,
		"username": StringFormatter,
		"email":    StringFormatter,
		"phone":    StringFormatter,
	},
}

// GetTable get table from .idb file
func GetTable(name string) *Table {
	return exampleTable
}

// get index key
func (t *Table) GetKey(row []string) []map[string]int64 {
	return nil
}

func (t *Table) Format(ast *SqlAST) [][]interface{} {
	res := make([][]interface{}, len(ast.Values))

	for idx1, row := range ast.Values {

		res[idx1] = make([]interface{}, len(row))

		if len(row) > len(t.Formatter) {
			panic("len(row) > len(t.formatter)")
		}

		for idx2, colData := range row {
			colName := ast.Columns[idx2]
			if t.Formatter[colName] == nil {
				panic("t.formatter[idx] == nil")
			}

			data := t.Formatter[colName](colData)
			res[idx1][idx2] = data
		}
	}
	return res
}

func (t *Table) CheckConstraint(ast *SqlAST) *ConstraintError {
	for idx, col := range ast.Columns {
		ast.Columns[idx] = strings.ToLower(col)
	}

	for _, row := range ast.Values {
		if len(row) > len(t.Constraint) {
			panic("len(row) > len(t.Constraint)")
		}

		for colIdx, colData := range row {
			colName := ast.Columns[colIdx]
			if t.Constraint[colName] == nil {
				continue
			}
			if err := t.Constraint[colName](colData); err != nil {
				return &ConstraintError{Table: t.Name, Column: t.Columns[colIdx], Err: err}
			}
		}
	}
	return nil
}

type ConstraintError struct {
	Table  string
	Column string
	Err    error
}
