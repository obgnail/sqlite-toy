package main

import (
	"fmt"
	sqlite "github.com/obgnail/sqlite_toy"
)

func main() {
	db := sqlite.NewDB()
	err := db.Insert(`INSERT INTO table (id, username, email) VALUES (27, auxten, "auxtenwpc_gmailcom")`)
	fmt.Println(err)
}
