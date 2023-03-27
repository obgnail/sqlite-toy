package sqlite

import (
	"fmt"
	"go/token"
	"go/types"
	"reflect"
	"strconv"
	"strings"
)

type Plan struct {
	table          *Table
	UnFilteredPipe chan *BPItem
	FilteredPipe   chan *BPItem
	LimitedPipe    chan *BPItem
	ErrorsPipe     chan error
	Stop           chan struct{}
}

func NewPlan(table *Table) (p *Plan) {
	return &Plan{
		table:          table,
		UnFilteredPipe: make(chan *BPItem),
		FilteredPipe:   make(chan *BPItem),
		LimitedPipe:    make(chan *BPItem),
		ErrorsPipe:     make(chan error, 1),
		Stop:           make(chan struct{}, 1),
	}
}

func (p *Plan) Delete(ast *DeleteAST) error {
	queryAST := &SelectAST{
		Table:    ast.Table,
		Projects: []string{ASTERISK},
		Where:    ast.Where,
		Limit:    ast.Limit,
	}
	rows, err := p.Select(queryAST)
	if err != nil {
		return err
	}

	tree := p.table.GetClusterIndex()
	for _, row := range rows {
		tree.Remove(row.Key)
	}
	return nil
}

func (p *Plan) Update(ast *UpdateAST) error {
	queryAST := &SelectAST{
		Table:    ast.Table,
		Projects: []string{ASTERISK},
		Where:    ast.Where,
		Limit:    ast.Limit,
	}
	rows, err := p.Select(queryAST)
	if err != nil {
		return err
	}

	var needReInsert bool
	for _, col := range ast.Columns {
		if col == p.table.PrimaryKey {
			needReInsert = true
			break
		}
	}

	if needReInsert {
		// update字段包含主键,并且待更新的记录大于1
		if len(rows) > 1 {
			return fmt.Errorf("update primaryKey, row > 2")
		}
		// 修改primaryKey的需要删除然后重新插入
		p.reInsert(rows[0], ast)
		return nil
	}

	for _, row := range rows {
		p.update(row, ast)
	}

	return nil
}

func (p *Plan) update(item *BPItem, ast *UpdateAST) {
	for idx1, col := range ast.Columns {
		for idx2, c := range p.table.Columns {
			if c == col {
				Val := item.Val.([]interface{})
				newVal := ast.NewValue[idx1]

				v := p.table.Formatter[c](newVal)
				Val[idx2] = v

				item.Val = Val
				break
			}
		}
	}
}

func (p *Plan) reInsert(item *BPItem, ast *UpdateAST) {
	newItem := &BPItem{Key: item.Key, Val: item.Key}
	for idx1, col := range ast.Columns {
		for idx2, c := range p.table.Columns {
			if c == col {
				Val := item.Val.([]interface{})
				newVal := ast.NewValue[idx1]
				Val[idx2] = newVal
				newItem.Val = Val
				break
			}
		}
	}

	tree := p.table.GetClusterIndex()
	tree.Remove(item.Key)
	update := tree.Set(newItem.Key, newItem.Val)
	if update {
		panic("updated!")
	}
}

func (p *Plan) Insert(dataset map[int64][]interface{}) error {
	tree := p.table.GetClusterIndex()

	for key, data := range dataset {
		val := tree.Get(key)
		if val != nil {
			return DuplicateKeyError
		}

		tree.Set(key, data)
	}
	return nil
}

func (p *Plan) Select(ast *SelectAST) (ret []*BPItem, err error) {
	// Fetch rows from storage pages
	tree := p.table.GetClusterIndex()
	if tree == nil {
		return nil, TableError
	}

	i := int64(0)
	// get all rows
	for row := range tree.GetAllItems() {
		// Filter rows according the ast.Where
		if len(ast.Where) != 0 {
			filtered, err := p.isRowFiltered(ast.Where, row)
			if err != nil {
				return nil, err
			}
			if filtered {
				continue
			}
		}

		// Count row count for LIMIT clause.
		i++
		if i > ast.Limit && ast.Limit > 0 {
			break
		}
		row = p.table.FilterCols(row, ast.Projects)
		ret = append(ret, row)
	}

	return
}

func (p *Plan) isRowFiltered(where []string, row *BPItem) (filtered bool, err error) {
	var (
		normalized = make([]string, len(where))
		tv         types.TypeAndValue
	)

	var cols []string
	for _, col := range p.table.Columns {
		cols = append(cols, strings.ToLower(col))
	}

Loop:
	for i, w := range where {
		switch upper := strings.ToLower(w); upper {
		case AND:
			normalized[i] = "&&"
			continue
		case OR:
			normalized[i] = "||"
			continue
		case "=":
			normalized[i] = "=="
			continue
		}

		for idx, col := range cols {
			if col == strings.ToLower(w) {
				value := row.Val.([]interface{})
				val := value[idx]
				rt := reflect.TypeOf(val)
				switch rt.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					normalized[i] = fmt.Sprintf("%d", val)
				case reflect.Bool:
					normalized[i] = fmt.Sprintf("%t", val)
				case reflect.String:
					normalized[i] = strconv.Quote(fmt.Sprintf("%s", val))
				default:
					normalized[i] = fmt.Sprintf("%v", val)
				}
				continue Loop
			}
		}

		normalized[i] = w
	}

	expr := strings.Join(normalized, " ")
	fSet := token.NewFileSet()
	if tv, err = types.Eval(fSet, nil, token.NoPos, expr); err != nil {
		return
	}
	if tv.Type == nil {
		err = fmt.Errorf("eval(%q) got nil type but no error", expr)
		return
	}
	if !strings.Contains(tv.Type.String(), "bool") {
		err = fmt.Errorf("eval(%q) got non bool type", expr)
		return
	}

	filtered = !(tv.Value.ExactString() == "true")
	return
}
