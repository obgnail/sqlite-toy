package sqlite

import (
	"strings"
)

// Table get table from .frm file
type Table struct {
	Name         string
	PrimaryKey   string
	Columns      []string
	Constraint   map[string]func(data string) error
	Formatter    map[string]func(data string) interface{}
	DefaultValue []interface{}
	Indies       map[string]*BPTree // multi indies, maybe
}

func (t *Table) GetClusterIndex() *BPTree {
	return t.Indies["-"]
}

// map[primaryKeyValue]rowData
// NOTE: 简单实现,限死prmaryKey必须是数字类型
func (t *Table) Format(ast *InsertAST) map[int64][]interface{} {
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

func (t *Table) fullZeroValue(astCols []string, astVal []interface{}) []interface{} {
	var data []interface{}
	for idx, col := range t.Columns {
		newVal := t.DefaultValue[idx]
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

func (t *Table) FilterCols(item *BPItem, cols []string) *BPItem {
	if len(cols) == 1 && cols[0] == ASTERISK {
		return item
	}

	val := make([]interface{}, 0, len(cols))
	for idx, col := range t.Columns {
		for _, filterCol := range cols {
			if filterCol == col {
				val = append(val, item.Val.([]interface{})[idx])
				break
			}
		}
	}
	result := &BPItem{Key: item.Key, Val: val}
	return result
}

func (t *Table) CheckSelectConstraint(ast *SelectAST) *ConstraintError {
	if err := t.CheckTable(ast.Table); err != nil {
		return err
	}

	cols := make(map[string]struct{}, len(t.Columns))
	for _, c := range t.Columns {
		cols[strings.ToLower(c)] = struct{}{}
	}

	for _, p := range ast.Projects {
		if p == ASTERISK {
			if len(ast.Projects) != 1 {
				return &ConstraintError{Table: t.Name, Err: SyntaxError}
			}
			break
		}

		if _, ok := cols[p]; !ok {
			return &ConstraintError{Table: t.Name, Err: HasNotColumnError}
		}
	}

	if err := t.CheckWhere(ast.Where); err != nil {
		return err
	}

	if err := t.CheckLimit(ast.Limit); err != nil {
		return err
	}

	return nil
}

func (t *Table) CheckDeleteConstraint(ast *DeleteAST) *ConstraintError {
	if err := t.CheckTable(ast.Table); err != nil {
		return err
	}
	if err := t.CheckWhere(ast.Where); err != nil {
		return err
	}

	if err := t.CheckLimit(ast.Limit); err != nil {
		return err
	}
	return nil
}

func (t *Table) CheckInsertConstraint(ast *InsertAST) *ConstraintError {
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

func (t *Table) CheckUpdateConstraint(ast *UpdateAST) *ConstraintError {
	if len(ast.NewValue) > len(t.Constraint) {
		panic("len(ast.NewValue) > len(t.Constraint)")
	}

	for idx, col := range ast.Columns {
		ast.Columns[idx] = strings.ToLower(col)
	}

	for idx, newVal := range ast.NewValue {
		colName := ast.Columns[idx]
		if t.Constraint[colName] == nil {
			continue
		}
		if err := t.Constraint[colName](newVal); err != nil {
			return &ConstraintError{Table: t.Name, Column: t.Columns[idx], Err: err}
		}
	}

	if err := t.CheckWhere(ast.Where); err != nil {
		return err
	}

	if err := t.CheckLimit(ast.Limit); err != nil {
		return err
	}

	return nil
}

func (t *Table) CheckWhere(where []string) *ConstraintError {
	cols := make(map[string]struct{}, len(t.Columns))
	for _, c := range t.Columns {
		cols[strings.ToLower(c)] = struct{}{}
	}

	needCheck := true
	for _, w := range where {
		if w == ";" {
			continue
		}
		if needCheck {
			if _, ok := cols[w]; !ok {
				return &ConstraintError{Table: t.Name, Err: HasNotColumnError}
			}
			needCheck = false
		}

		if w == AND || w == OR {
			needCheck = true
		}
	}
	return nil
}

func (t *Table) CheckLimit(limit int64) *ConstraintError {
	if limit < 0 {
		return &ConstraintError{Table: t.Name, Err: SyntaxError}
	}
	return nil
}

func (t *Table) CheckTable(table string) *ConstraintError {
	if table != t.Name {
		return &ConstraintError{Table: t.Name, Err: TableError}
	}
	return nil
}

type ConstraintError struct {
	Table  string
	Row    []string
	Column string
	Err    error
}
