package main

import (
	"fmt"
	sqlite "github.com/obgnail/sqlite_toy"
)

func main() {
	table := &sqlite.Table{
		Name:       "user",
		PrimaryKey: "id",
		Columns:    []string{"id", "sex", "age", "username", "email", "phone"},
		Constraint: map[string]func(data string) error{
			"id": sqlite.Compose(sqlite.IsInteger, sqlite.NotEmpty),
			"sex": func(data string) error {
				return sqlite.OptionLimit[string](sqlite.TrimQuotes(data), []string{"male", "female"})
			},
			"age":      sqlite.IsSignedInteger,
			"username": func(data string) error { return sqlite.VarcharTooLong(data, 8) },
			"email":    sqlite.IsString,
			"phone":    sqlite.IsString,
		},
		Formatter: map[string]func(data string) interface{}{
			"id":       sqlite.IntegerFormatter,
			"sex":      sqlite.StringFormatter,
			"age":      sqlite.IntegerFormatter,
			"username": sqlite.StringFormatter,
			"email":    sqlite.StringFormatter,
			"phone":    sqlite.StringFormatter,
		},
		ZeroValue: []interface{}{"", "male", 0, "", "", ""},
		Indies: map[string]*sqlite.BPTree{
			"-": sqlite.NewBPTree(17, nil),
		},
	}

	db := sqlite.NewDB()
	db.AddTable(table)

	err := db.Exec(`INSERT INTO user (id, username, email) VALUES (27, "userName", "user@gmail.com")`)
	if err != nil {
		panic(err)
	}

	result, err := db.Query(`SELECT id,username FROM user WHERE id > 3 LIMIT 10`)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
