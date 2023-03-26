package sqlite

type Plan struct {
	db    *DB
	table *Table
}

func NewPlan(db *DB) (p *Plan) {
	return &Plan{db: db}
}

func (p *Plan) Insert(dataset map[int64][]interface{}) error {
	for key, data := range dataset {
		val := p.db.Tree.Get(key)
		if val != nil {
			return DuplicateKeyError
		}

		p.db.Tree.Set(key, data)
	}
	return nil
}
