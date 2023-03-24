package main

import (
	"fmt"
	sqlite "github.com/obgnail/sqlite_toy"
)

func main() {
	db, err := sqlite.Open("./test.db")
	if err != nil {
		panic(err)
	}
	fmt.Println(db)
}
