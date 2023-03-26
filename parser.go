package sqlite

import (
	"fmt"
	"github.com/pingcap/errors"
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

	FROM     = "FROM"
	WHERE    = "WHERE"
	LIMIT    = "LIMIT"
	INTO     = "INTO"
	VALUES   = "VALUES"
	ASTERISK = "*"
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

type InsertTree struct {
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
func (p *Parser) ParseInsert(insert string) (ast *InsertTree, err error) {
	p.s.Init(strings.NewReader(insert))
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars | scanner.ScanStrings | scanner.ScanRawStrings

	if !p.scanAndCheck(&p.s, INSERT) {
		return nil, fmt.Errorf("not INSERT statement")
	}
	if !p.scanAndCheck(&p.s, INTO) {
		return nil, fmt.Errorf("expect INTO after INSERT")
	}

	ast = &InsertTree{}

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

		columns, err := p.getColumns(&p.s)
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
			row, err := p.getColumns(&p.s)
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
func (p *Parser) getColumns(s *scanner.Scanner) ([]string, error) {
	columns := make([]string, 0, 8)

	for {
		if tok := s.Scan(); tok == scanner.EOF {
			return nil, fmt.Errorf("get Columns failed")
		}
		txt := s.TokenText()
		txt = strings.ToUpper(txt)

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
