package main

import (
	sqlite "github.com/obgnail/sqlite_toy"
)

func main() {
	//table := &sqlite.Table{
	//	Name:       "user",
	//	PrimaryKey: "id",
	//	Columns:    []string{"id", "sex", "age", "username", "email", "phone"},
	//	Constraint: map[string]func(data string) error{
	//		"id": sqlite.Compose(sqlite.IsInteger, sqlite.NotEmpty),
	//		"sex": func(data string) error {
	//			return sqlite.OptionLimit[string](sqlite.TrimQuotes(data), []string{"male", "female"})
	//		},
	//		"age":      sqlite.IsSignedInteger,
	//		"username": func(data string) error { return sqlite.VarcharTooLong(data, 8) },
	//		"email":    sqlite.IsString,
	//		"phone":    sqlite.IsString,
	//	},
	//	Formatter: map[string]func(data string) interface{}{
	//		"id":       sqlite.IntegerFormatter,
	//		"sex":      sqlite.StringFormatter,
	//		"age":      sqlite.IntegerFormatter,
	//		"username": sqlite.StringFormatter,
	//		"email":    sqlite.StringFormatter,
	//		"phone":    sqlite.StringFormatter,
	//	},
	//	DefaultValue: []interface{}{"", "male", 0, "", "", ""},
	//	Indies: map[string]*sqlite.BPTree{
	//		"-": sqlite.NewBPTree(17, nil),
	//	},
	//}

	db := sqlite.NewDB()
	err := db.Exec(`
	CREATE TABLE user (
		email      VARCHAR(255)   NOT NULL  DEFAULT "default@gmail.com",
		username   VARCHAR(8)     NOT NULL,
		id         INTEGER        NOT NULL,
		PRIMARY KEY (id)
	);`)

	//err := db.Exec(`
	//CREATE TABLE user (
	//	id         INTEGER        NOT NULL,
	//	username   VARCHAR(8)     NOT NULL,
	//	email      VARCHAR(255)   NOT NULL  DEFAULT "default@gmail.com",
	//	PRIMARY KEY (id)
	//);`)
	if err != nil {
		panic(err)
	}

	//db.AddTable(table)
	//
	//err := db.Exec(`INSERT INTO user (id, username, email) VALUES (27, "userName", "User@gmail.com")`)
	//if err != nil {
	//	panic(err)
	//}
	//
	//re := table.GetClusterIndex().Get(27)
	//fmt.Println(re)

	//result, err := db.Query(`SELECT id,username,email FROM user WHERE id > 3 LIMIT 10`)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(result)

	//err = db.Exec(`UPDATE user SET username = "newName", email = "NewEmail" WHERE id > 3 LIMIT 10;`)
	//if err != nil {
	//	panic(err)
	//}
	//
	//re := table.GetClusterIndex().Get(27)
	//fmt.Println(re)

	//err = db.Exec(`DELETE FROM user WHERE username= "userName" AND email="User@gmail.com"`)
	//if err != nil {
	//	errors.ErrorStack(errors.Trace(err))
	//	return
	//}
	//
	//re = table.GetClusterIndex().Get(27)
	//fmt.Println(re)
}
