package sqlite

import (
	"strings"
)

// Table get table from .frm file
type Table struct {
	Name       string
	PrimaryKey string
	Columns    []string
	Constraint map[string]func(data string) error
	Formatter  map[string]func(data string) interface{}
	ZeroValue  []interface{}
	Indies     map[string]*BPTree // multi indies, maybe
}

func (t *Table) GetClusterIndex() *BPTree {
	return t.Indies["-"]
}

// map[primaryKeyValue]rowData
// NOTE: 简单实现,限死prmaryKey必须是数字类型
func (t *Table) Format(ast *SqlAST) map[int64][]interface{} {
	vals := make([][]interface{}, 0, len(ast.Values))
	for rowIdx, row := range ast.Values {
		if len(row) > len(t.Formatter) {
			panic("len(row) > len(t.formatter)")
		}

		vals = append(vals, make([]interface{}, len(ast.Columns)))

		for colIdx, colData := range row {
			colName := ast.Columns[colIdx]
			if t.Formatter[colName] == nil {
				panic("t.formatter[idx] == nil")
			}
			data := t.Formatter[colName](colData)
			vals[rowIdx][colIdx] = data
		}
	}

	var fullColVals [][]interface{}
	for rowIdx := range ast.Values {
		data := t.fullZeroValue(ast.Columns, vals[rowIdx])
		fullColVals = append(fullColVals, data)
	}

	res := make(map[int64][]interface{})
	for _, rowVals := range fullColVals {
		for colIdx, val := range rowVals {
			if t.Columns[colIdx] == t.PrimaryKey {
				k, ok := val.(int)
				if !ok {
					panic("get primary key err")
				}
				key := int64(k)
				res[key] = rowVals
				break
			}
		}
	}

	return res
}

//func (t *Table) fullColumns(columns []string, values []interface{}) map[int64][]interface{} {
//	var fullColVals [][]interface{}
//	for rowIdx := range ast.Values {
//		data := t.fullZeroValue(columns, vals[rowIdx])
//		fullColVals = append(fullColVals, data)
//	}
//
//	res := make(map[int64][]interface{})
//	for _, rowVals := range fullColVals {
//		for colIdx, val := range rowVals {
//			if t.Columns[colIdx] == t.PrimaryKey {
//				k, ok := val.(int)
//				if !ok {
//					panic("get primary key err")
//				}
//				key := int64(k)
//				res[key] = rowVals
//				break
//			}
//		}
//	}
//
//}

func (t *Table) fullZeroValue(astCols []string, astVal []interface{}) []interface{} {
	var data []interface{}
	for idx, col := range t.Columns {
		newVal := t.ZeroValue[idx]
		for astIdx, astCol := range astCols {
			if astCol == col {
				newVal = astVal[astIdx]
				break
			}
		}
		data = append(data, newVal)
	}
	return data
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
				return &ConstraintError{Table: t.Name, Row: row, Column: t.Columns[idx], Err: err}
			}
		}

		if primaryKeyIdx == -1 {
			return &ConstraintError{Table: t.Name, Row: row, Column: t.Columns[primaryKeyIdx], Err: HasNoPrimaryKeyError}
		}
	}
	return nil
}

type ConstraintError struct {
	Table  string
	Row    []string
	Column string
	Err    error
}
