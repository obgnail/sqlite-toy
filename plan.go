package sqlite

type Plan struct {
	db    *DB
	table *Table
}

func NewPlan(db *DB) (p *Plan) {
	return &Plan{db: db}
}

func (p *Plan) Insert(ast *SqlAST, dataset [][]interface{}) error {
	//p.db.Tree.Set()
	return nil
}
