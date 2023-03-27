# sqlite-toy



## 简介

`sqlite-toy` 是一个**以研究为目的的**，**基于内存的**，**完全原生实现的**，有限支持 SQL 查询的关系型数据库。

主要的目标是为了向数据库爱好者展示一个关系型数据库的基本原理和关键设计。因此，为了便于理解，采取了很多取巧但不是很严谨的设计，代码量控制在 2000 行以内。



## 特性列表

纯 Golang 实现，不依赖任何第三方包。



#### 存储引擎

基于 B+Tree 的数据检索结构。

#### SQL Parser

1. Tokenizer 基于 text/scanner 实现。
2. 支持简单的 SELECT、INSERT、UPDATE、DELETE、CREARE TABLE 语法。
   1. SELECT、UPDATE、DELETE 支持数值类型的 WHERE。
   2. 支持 LIMIT，但暂不支持 ORDER BY。
3. 距离实现 SQL-2011 标准有十万八千里远。

#### 执行计划 Planner

基于火山模型（Volcano Model）的 Select 实现。



## 实现的局限

1. 有限支持 SQL 语法。
2. 以研究为目的，没有严格的单元测试。
3. Tokenizer 由于是基于 Golang 语言本身的一个取巧实现，对于一些字符串里的特殊字符支持会出现问题，可以通过加 `"` 解决。



## 使用

```go
func main() {
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

	result, err := db.Query(`SELECT email, id, username FROM user WHERE id > 3 LIMIT 10`)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(result)

	err = db.Exec(`UPDATE user SET username = "newName222", email = "NewEmail111" WHERE username = "userName-27";`)
	if err != nil {
		log.Fatalln(err)
	}

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
}
```

