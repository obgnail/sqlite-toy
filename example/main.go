package main

import (
	"encoding/json"
	"fmt"
	sqlite "github.com/obgnail/sqlite_toy"
	"log"
)

func main() {
	example1()
	example2()
}

func example1() {
	db := sqlite.NewDB()
	err := db.Exec(`
	CREATE TABLE user (
		email      VARCHAR(255)   NOT NULL  DEFAULT "default@gmail.com",
		username   VARCHAR(16)    NOT NULL,
		id         INTEGER        NOT NULL,
		PRIMARY KEY (id)
	);`)
	if err != nil {
		log.Fatalln(err)
	}

	for i := 1; i != 30; i++ {
		sql := fmt.Sprintf(`INSERT INTO user (id, username, email) VALUES (%d, "userName-%d", "User-%d@gmail.com")`, i, i, i)
		if err = db.Exec(sql); err != nil {
			log.Fatalln(err)
		}
	}

	tree := db.Tables["user"].GetClusterIndex()

	showTree(tree)

	res := tree.Get(27)
	fmt.Println(res)

	result, err := db.Query(`SELECT email, id, username FROM user WHERE id > 3 LIMIT 10`)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(result)

	err = db.Exec(`UPDATE user SET username = "newName222", email = "NewEmail111" WHERE username = "userName-27";`)
	if err != nil {
		log.Fatalln(err)
	}

	re := tree.Get(27)
	fmt.Println(re)

	err = db.Exec(`DELETE FROM user WHERE username = "newName222" AND email = "NewEmail111";`)
	if err != nil {
		log.Fatalln(err)
	}

	err = db.Exec(`DELETE FROM user WHERE id < 25;`)
	if err != nil {
		log.Fatalln(err)
	}

	result, err = db.Query(`SELECT email, id, username FROM user WHERE id > 26`)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(result)

	fmt.Println("------")

	showTree(tree)

	fmt.Println("******")

	for item := range tree.GetAllItems() {
		fmt.Println(item)
	}
}
func example2() {
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
			"username": func(data string) error { return sqlite.VarcharTooLong(data, 16) },
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
		DefaultValue: []interface{}{"", "male", 0, "", "", ""},
		Indies: map[string]*sqlite.BPTree{
			"-": sqlite.NewBPTree(17, nil),
		},
	}

	db := sqlite.NewDB()
	db.AddTable(table)

	for i := 1; i != 30; i++ {
		sql := fmt.Sprintf(`INSERT INTO user (id, username, email) VALUES (%d, "userName-%d", "User-%d@gmail.com")`, i, i, i)
		if err := db.Exec(sql); err != nil {
			log.Fatalln(err)
		}
	}

	tree := db.Tables["user"].GetClusterIndex()

	showTree(tree)

	res := tree.Get(27)
	fmt.Println(res)

	result, err := db.Query(`SELECT email, id, username FROM user WHERE id > 20`)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(result)
}

func showTree(tree *sqlite.BPTree) {
	result := tree.GetData()
	data, _ := json.MarshalIndent(result, "", "    ")
	fmt.Println(string(data))
}
