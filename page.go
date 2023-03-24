package sqlite

import (
	"fmt"
	"github.com/auxten/go-sqldb/node"
	"github.com/juju/errors"
	"io"
	"os"
)

const (
	PageSize = 4096 // 4k分页
)

type Page struct {
	node Node
}

func UnmarshalPage(buf []byte) (*Page, error) {
	var n Node = &LeafNode{}
	if buf[0] == 0 {
		n = &NonLeafNode{}
	}

	if _, err := n.Unmarshal(buf); err != nil {
		return nil, errors.Trace(err)
	}
	return &Page{node: n}, nil
}

type Pager struct {
	File    *os.File
	fileLen int64
	PageNum int
	cache   map[int]*Page // map[pageIdx]*Page
}

func PagerOpen(fileName string) (pager *Pager, err error) {
	var dbFile *os.File
	var fileLen int64

	if dbFile, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600); err != nil {
		return nil, errors.Trace(err)
	}

	// get file length
	if fileLen, err = dbFile.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Trace(err)
	}

	if fileLen%PageSize != 0 {
		return nil, fmt.Errorf("dbFile length must be n * PageSize")
	}

	pageNum := int(fileLen / PageSize)

	pager = &Pager{
		File:    dbFile,
		fileLen: fileLen,
		PageNum: pageNum,
		cache:   make(map[int]*Page),
	}

	return
}

func (p *Pager) GetPage(pageIdx int) (page *Page, err error) {
	if page = p.cache[pageIdx]; page != nil {
		return page, nil
	}

	// Cache miss
	// If pageIdx within data file, just read,
	// else just return blank page which will be flushed to db file later.
	buf := make([]byte, PageSize)
	// 从第几页开始读起,读取一页的内容
	if _, err = p.File.ReadAt(buf, int64(pageIdx*PageSize)); err != nil {
		if err != io.EOF {
			err = errors.Trace(err)
			return
		}
	}

	page, err = UnmarshalPage(buf)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	p.cache[pageIdx] = page
	if pageIdx >= p.PageNum {
		p.PageNum = pageIdx + 1
	}
	return page, nil
}

func (p *Pager) FlushPage(pageIdx int) (err error) {
	page := p.cache[pageIdx]
	if page == nil {
		return fmt.Errorf("flushing nil page")
	}

	buf, err := page.node.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = p.File.WriteAt(buf, int64(pageIdx*node.PageSize))
	if err != nil {
		return errors.Trace(err)
	}
	return
}
