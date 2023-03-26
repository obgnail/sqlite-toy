package sqlite

type Table struct {
	Name       string
	Columns    []string
	Constraint []func(data string) error
	formatter  []func(data string) interface{}
	indies     []map[string]int64 // map[columnName]int64, multi indies,maybe
}

// GetTable get table from .idb
func GetTable(name string) *Table {
	return &Table{
		Name:       "user",
		Columns:    []string{"id", "sex", "age", "username", "email", "phone"},
		Constraint: nil,
	}
}

// get index key
func (t *Table) GetKey(row []string) []map[string]int64 {
	return nil
}

func (t *Table) Format(dataset [][]string) [][]interface{} {
	res := make([][]interface{}, len(dataset))

	for idx1, row := range dataset {

		res[idx1] = make([]interface{}, len(row))

		if len(row) > len(t.formatter) {
			panic("len(row) > len(t.formatter)")
		}

		for idx2, colData := range row {
			if t.formatter[idx2] == nil {
				panic("t.formatter[idx] == nil")
			}

			data := t.formatter[idx2](colData)
			res[idx1][idx2] = data
		}
	}
	return res
}

func (t *Table) CheckConstraint(dataset [][]string) *ConstraintError {
	for _, row := range dataset {
		if len(row) > len(t.Constraint) {
			panic("len(row) > len(t.Constraint)")
		}
		for idx, colData := range row {
			if t.Constraint[idx] == nil {
				continue
			}
			if err := t.Constraint[idx](colData); err != nil {
				return &ConstraintError{Table: t.Name, Column: t.Columns[idx], Err: err}
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
