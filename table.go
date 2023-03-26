package sqlite

import (
	"strconv"
	"strings"
)

type Table struct {
	Name       string
	PrimaryKey string
	Columns    []string
	Constraint map[string]func(data string) error
	Formatter  map[string]func(data string) interface{}
	Indies     []map[string]int64 // map[columnName]int64, multi indies,maybe
}

var exampleTable = &Table{
	Name:       "user",
	PrimaryKey: "id",
	Columns:    []string{"id", "sex", "age", "username", "email", "phone"},
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

// map[primaryKeyValue]rowData
// NOTE: 简单实现,限死prmaryKey必须是数字类型
func (t *Table) Format(ast *SqlAST) map[int64][]interface{} {
	res := make(map[int64][]interface{}, len(ast.Values))

	var key int64
	for _, row := range ast.Values {
		if len(row) > len(t.Formatter) {
			panic("len(row) > len(t.formatter)")
		}

		for colIdx, colData := range row {
			colName := ast.Columns[colIdx]

			if t.Formatter[colName] == nil {
				panic("t.formatter[idx] == nil")
			}

			data := t.Formatter[colName](colData)

			if colName == t.PrimaryKey {
				k, err := strconv.Atoi(data.(string))
				if err != nil {
					panic(err)
				}
				key = int64(k)
			}
			res[key] = append(res[key], data)
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

		var primaryKeyIdx = -1

		for idx, colData := range row {
			colName := ast.Columns[idx]

			// NOTE: 简单实现, 限死primary只能是一个col
			if colName == t.PrimaryKey {
				primaryKeyIdx = idx
			}

			if t.Constraint[colName] == nil {
				continue
			}
			if err := t.Constraint[colName](colData); err != nil {
				return &ConstraintError{Table: t.Name, Column: t.Columns[idx], Err: err}
			}
		}

		if primaryKeyIdx == -1 {
			return &ConstraintError{Table: t.Name, Column: t.Columns[primaryKeyIdx], Err: HasNoPrimaryKeyError}
		}
	}
	return nil
}

type ConstraintError struct {
	Table  string
	Column string
	Err    error
}
