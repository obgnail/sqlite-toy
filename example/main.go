package main

import (
	"fmt"
	sqlite "github.com/obgnail/sqlite_toy"
)

func main() {
	db := sqlite.NewDB()
	err := db.Insert(`INSERT INTO table (id, username, email) VALUES (27, "userName", "user@gmail.com")`)
	if err != nil {
		panic(err)
	}

	result := db.Tree.Get(27)

	fmt.Println(result)
}
