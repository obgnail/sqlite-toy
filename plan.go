package sqlite

type Plan struct {
	table *Table
}

func NewPlan(table *Table) (p *Plan) {
	return &Plan{table: table}
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
