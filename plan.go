package sqlite

type Plan struct {
	table          *Table
	UnFilteredPipe chan interface{}
	FilteredPipe   chan interface{}
	LimitedPipe    chan interface{}
	ErrorsPipe     chan error
	Stop           chan bool
}

func NewPlan(table *Table) (p *Plan) {
	return &Plan{
		table:          table,
		UnFilteredPipe: make(chan interface{}),
		FilteredPipe:   make(chan interface{}),
		LimitedPipe:    make(chan interface{}),
		ErrorsPipe:     make(chan error, 1),
		Stop:           make(chan bool, 1),
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

func (p *Plan) Select(ast *SelectAST) error {
	return nil
}
