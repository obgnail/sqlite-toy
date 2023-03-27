package sqlite

import (
	"fmt"
	"github.com/pingcap/errors"
	"strconv"
	"strings"
	"text/scanner"
)

type StatementType string

// SQL type tokens
const (
	UNSUPPORTED = "N/A"
	SELECT      = "SELECT"
	INSERT      = "INSERT"
	UPDATE      = "UPDATE"
	DELETE      = "DELETE"

	FROM   = "FROM"
	WHERE  = "WHERE"
	LIMIT  = "LIMIT"
	INTO   = "INTO"
	VALUES = "VALUES"
	Set    = "SET"

	ASTERISK = "*"
	AND      = "and"
	OR       = "or"
)

type Parser struct {
	s scanner.Scanner
}

func (p *Parser) GetSQLType(sql string) StatementType {
	s := p.s
	s.Init(strings.NewReader(sql))
	s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars | scanner.ScanStrings | scanner.ScanRawStrings

	if tok := s.Scan(); tok != scanner.EOF {
		txt := strings.ToUpper(s.TokenText())
		switch txt {
		case "SELECT":
			return SELECT
		case "INSERT":
			return INSERT
		case "UPDATE":
			return UPDATE
		case "DELETE":
			return DELETE
		default:
			return UNSUPPORTED
		}
	}

	return UNSUPPORTED
}

type InsertAST struct {
	Table   string
	Columns []string
	Values  [][]string
}

/*
ParseInsert can parse a simple INSERT statement, eg.
 	INSERT INTO table_name VALUES (value1, value2, …)
	or
	INSERT INTO table_name(column1, column2, …) VALUES (value1, value2, …)
*/
func (p *Parser) ParseInsert(insert string) (ast *InsertAST, err error) {
	p.s.Init(strings.NewReader(insert))
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars | scanner.ScanStrings | scanner.ScanRawStrings

	if !p.scanAndCheck(&p.s, INSERT) {
		return nil, fmt.Errorf("not INSERT statement")
	}
	if !p.scanAndCheck(&p.s, INTO) {
		return nil, fmt.Errorf("expect INTO after INSERT")
	}

	ast = &InsertAST{}

	// Table
	if tok := p.s.Scan(); tok == scanner.EOF {
		return nil, fmt.Errorf("%s expect table after INSERT INTO", insert)
	}
	ast.Table = p.s.TokenText()

	// ColNames
	if tok := p.s.Scan(); tok == scanner.EOF {
		return nil, fmt.Errorf("%s expect VALUES or (colNames)", insert)
	}

	txt := strings.ToUpper(p.s.TokenText())
	if txt != VALUES {
		if txt != "(" {
			return nil, fmt.Errorf("%s expect VALUES or (colNames)", insert)
		}

		columns, err := p.scanColumns(&p.s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ast.Columns = columns

		if !p.scanAndCheck(&p.s, VALUES) {
			return nil, fmt.Errorf("%s expect VALUES", insert)
		}
	}

	// Values
	columnCnt := len(ast.Columns)
	// VALUES has been scanned try to get (value1, value2), (value3, value4)
	if columnCnt != 0 {
		ast.Values = make([][]string, 0, columnCnt)
	} else {
		ast.Values = make([][]string, 0, 8)
	}

	for {
		if tok := p.s.Scan(); tok == scanner.EOF {
			break
		}

		txt := p.s.TokenText()
		if txt == "," {
			continue // next row
		}
		if txt == "(" {
			row, err := p.scanColumns(&p.s)
			if err != nil {
				return nil, errors.Trace(err)
			}
			ast.Values = append(ast.Values, row)
		} else {
			return nil, fmt.Errorf("%s expected (", insert)
		}
	}

	// Check if column count identical
	for _, row := range ast.Values {
		if columnCnt == 0 {
			columnCnt = len(ast.Values[0]) // compare with first row
		}
		if columnCnt != len(row) {
			err = fmt.Errorf(
				"%s expected column count is %d, got %d, %v",
				insert, columnCnt, len(row), row,
			)
			return
		}
	}

	return
}

func (p *Parser) scanAndCheck(s *scanner.Scanner, target string) bool {
	tok := s.Scan()
	return tok != scanner.EOF && strings.ToUpper(s.TokenText()) == target
}

// (col1,col2,col3)
func (p *Parser) scanColumns(s *scanner.Scanner) ([]string, error) {
	columns := make([]string, 0, 8)

	for {
		if tok := s.Scan(); tok == scanner.EOF {
			return nil, fmt.Errorf("get Columns failed")
		}
		txt := s.TokenText()
		if txt == "," || txt == "(" {
			continue
		} else if txt == ")" {
			break
		} else {
			columns = append(columns, txt)
		}
	}

	return columns, nil
}

func (p *Parser) ScanWhere(s *scanner.Scanner) ([]string, string, error) {
	var where []string
	var lastToken string
	for {
		if tok := s.Scan(); tok == scanner.EOF {
			if len(where) == 0 {
				return nil, lastToken, fmt.Errorf("missing WHERE clause")
			}
			return where, lastToken, nil
		}
		txt := p.s.TokenText()
		if strings.ToUpper(txt) == LIMIT {
			lastToken = LIMIT
			break
		}
		where = append(where, txt)
	}
	return where, lastToken, nil
}

type SelectAST struct {
	Table    string
	Projects []string
	Where    []string
	Limit    int64
}

/*
ParseSelect is a simple select statement parser.
It's just a demo of SELECT statement parser skeleton.
Currently, the most complex SQL supported here is something like:

	SELECT * FROM foo WHERE id < 3 LIMIT 1;

Even SQL-92 standard is far more complex.
For a production ready SQL parser, see: https://github.com/auxten/postgresql-parser
*/
func (p *Parser) ParseSelect(sql string) (ast *SelectAST, err error) {
	p.s.Init(strings.NewReader(sql))
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars | scanner.ScanStrings | scanner.ScanRawStrings

	if !p.scanAndCheck(&p.s, SELECT) {
		err = fmt.Errorf("%s is not SELECT statement", sql)
		return
	}

	ast = &SelectAST{Projects: make([]string, 0, 4)}

	for {
		if tok := p.s.Scan(); tok == scanner.EOF {
			if len(ast.Projects) == 0 {
				err = fmt.Errorf("%s get select projects failed", sql)
			}
			return
		} else {
			txt := p.s.TokenText()
			if txt == ASTERISK {
				ast.Projects = append(ast.Projects, ASTERISK)
			} else {
				if txt == "," {
					continue
				} else if strings.ToUpper(txt) == FROM {
					break
				} else {
					ast.Projects = append(ast.Projects, strings.ToLower(txt))
				}
			}
		}
	}

	// token FROM is scanned, try to get the table name here
	// FROM ?
	if tok := p.s.Scan(); tok == scanner.EOF {
		// if projects are all constant value, source table is not necessary.
		// eg.  SELECT 1;
		return
	} else {
		ast.Table = strings.ToLower(p.s.TokenText())
	}

	// WHERE
	if tok := p.s.Scan(); tok == scanner.EOF {
		// WHERE/Limit is not necessary
		return
	}

	txt := p.s.TokenText()
	txt = strings.ToUpper(txt)
	var lastToken string
	if txt == WHERE {
		// token WHERE is scanned, try to get the WHERE clause.
		ast.Where, lastToken, err = p.ScanWhere(&p.s)
		if err != nil {
			return nil, err
		}
	} else if txt != LIMIT {
		err = fmt.Errorf("expect WHERE or LIMIT here")
		return
	}

	if lastToken == LIMIT || txt == LIMIT {
		// token LIMIT is scanned, try to get the limit
		if tok := p.s.Scan(); tok == scanner.EOF {
			err = fmt.Errorf("expect LIMIT clause here")
			return
		}
		txt = p.s.TokenText()
		ast.Limit, err = strconv.ParseInt(txt, 10, 64)
	}

	return
}

type UpdateAST struct {
	Table    string
	Columns  []string
	NewValue []string
	Where    []string
	Limit    int64
}

func (p *Parser) ParseUpdate(sql string) (ast *UpdateAST, err error) {
	p.s.Init(strings.NewReader(sql))
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars | scanner.ScanStrings | scanner.ScanRawStrings

	if !p.scanAndCheck(&p.s, UPDATE) {
		err = fmt.Errorf("%s is not UPDATE statement", sql)
		return
	}

	ast = &UpdateAST{}

	// Table
	if tok := p.s.Scan(); tok == scanner.EOF {
		return nil, fmt.Errorf("%s expect table after Update", sql)
	}
	ast.Table = p.s.TokenText()

	if !p.scanAndCheck(&p.s, Set) {
		return nil, fmt.Errorf("expect INTO after INSERT")
	}

	var lastToken string
	ast.Columns, ast.NewValue, lastToken, err = p.ScanSet(&p.s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ast.Where, ast.Limit, err = p.ScanWhereAndLimit(&p.s, lastToken)
	return ast, err
}

func (p *Parser) ScanSet(s *scanner.Scanner) ([]string, []string, string, error) {
	var cols []string
	var vals []string
	var lastToken string
	for {
		if tok := s.Scan(); tok == scanner.EOF {
			if len(cols) == 0 {
				return cols, vals, lastToken, fmt.Errorf("missing WHERE clause")
			}
			return cols, vals, lastToken, nil
		}

		txt := strings.ToUpper(p.s.TokenText())
		if txt == "," {
			continue
		} else if txt == WHERE || txt == LIMIT {
			lastToken = txt
			break
		}

		col := txt

		if !p.scanAndCheck(s, "=") {
			return cols, vals, lastToken, fmt.Errorf("expect = in sql")
		}

		if tok := p.s.Scan(); tok == scanner.EOF {
			return cols, vals, lastToken, fmt.Errorf("%s expect new value after =")
		}
		newValue := p.s.TokenText()

		cols = append(cols, col)
		vals = append(vals, newValue)
	}

	return cols, vals, lastToken, nil
}

func (p *Parser) ScanWhereAndLimit(s *scanner.Scanner, lastToken string) (where []string, limit int64, err error) {
	var last string
	if lastToken == WHERE {
		where, last, err = p.ScanWhere(s)
		if err != nil {
			return
		}
	} else if lastToken != LIMIT {
		err = fmt.Errorf("expect WHERE or LIMIT here")
		return
	}

	if lastToken == LIMIT || last == LIMIT {
		if tok := s.Scan(); tok == scanner.EOF {
			err = fmt.Errorf("expect LIMIT clause here")
			return
		}
		txt := s.TokenText()
		limit, err = strconv.ParseInt(txt, 10, 64)
	}
	return
}

type DeleteAST struct {
	Table string
	Where []string
	Limit int64
}

func (p *Parser) ParseDelete(sql string) (ast *DeleteAST, err error) {
	p.s.Init(strings.NewReader(sql))
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars | scanner.ScanStrings | scanner.ScanRawStrings

	if !p.scanAndCheck(&p.s, DELETE) {
		err = fmt.Errorf("%s is not DELETE statement", sql)
		return
	}
	if !p.scanAndCheck(&p.s, FROM) {
		err = fmt.Errorf("%s is not FROM statement", sql)
		return
	}

	ast = &DeleteAST{}

	// Table
	if tok := p.s.Scan(); tok == scanner.EOF {
		return nil, fmt.Errorf("%s expect table after Update", sql)
	}
	ast.Table = p.s.TokenText()

	if tok := p.s.Scan(); tok == scanner.EOF {
		return nil, fmt.Errorf("%s expect table after Update", sql)
	}
	lastToken := strings.ToUpper(p.s.TokenText())
	ast.Where, ast.Limit, err = p.ScanWhereAndLimit(&p.s, lastToken)
	return
}
