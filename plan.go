package sqlite

import (
	"fmt"
	"github.com/juju/errors"
	"go/token"
	"go/types"
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

func (p *Plan) Update(ast *UpdateAST) error {
	queryAST := &SelectAST{
		Table:    ast.Table,
		Projects: []string{ASTERISK},
		Where:    ast.Where,
		Limit:    ast.Limit,
	}
	result, err := p.Select(queryAST)
	if err != nil {
		return errors.Trace(err)
	}

	fmt.Println(result)

	return nil
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

	// get all rows
	go func() {
		for row := range tree.GetAllItems() {
			row = p.table.FilterCols(row, ast.Projects)
			p.UnFilteredPipe <- row
		}
		close(p.UnFilteredPipe)
	}()

	// Filter rows according the ast.Where
	go func(where []string) {
		for row := range p.UnFilteredPipe {
			if len(where) == 0 {
				p.FilteredPipe <- row
				continue
			}
			filtered, err := p.isRowFiltered(where, row)
			if err != nil {
				p.ErrorsPipe <- err
				return
			}
			if !filtered {
				p.FilteredPipe <- row
			}
		}
		close(p.FilteredPipe)
	}(ast.Where)

	// Count row count for LIMIT clause.
	go func(limit int64) {
		i := int64(0)
		for row := range p.FilteredPipe {
			i++
			if i > limit && limit > 0 {
				return
			}
			p.LimitedPipe <- row
		}
		p.Stop <- struct{}{}
		close(p.LimitedPipe)
	}(ast.Limit)

	// wait result
	wait := make(chan struct{}, 1)
	go func(err error) {
		for {
			select {
			case row := <-p.LimitedPipe:
				if row != nil {
					ret = append(ret, row)
				}
			case err = <-p.ErrorsPipe:
				return
			case <-p.Stop:
				wait <- struct{}{}
				return
			}
		}
	}(err)
	<-wait

	return
}

func (p *Plan) isRowFiltered(where []string, row *BPItem) (filtered bool, err error) {
	var (
		normalized = make([]string, len(where))
		tv         types.TypeAndValue
	)

	var cols []string
	for _, col := range p.table.Columns {
		cols = append(cols, strings.ToUpper(col))
	}

Loop:
	for i, w := range where {
		upper := strings.ToUpper(w)

		if upper == AND {
			normalized[i] = "&&"
			continue
		}

		if upper == OR {
			normalized[i] = "||"
			continue
		}

		for idx, col := range cols {
			if col == upper {
				value := row.Val.([]interface{})
				val := value[idx]
				normalized[i] = fmt.Sprintf("%v", val)
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
