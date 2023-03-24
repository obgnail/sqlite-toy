package sqlite

import (
	"fmt"
	"github.com/juju/errors"
	"io"
	"os"
)

const (
	PageSize = 4096 // 4k分页
)

type Page struct {
	// Either NonLeafNode or LeafNode
	NonLeafNode *NonLeafNode
	LeafNode    *LeafNode
}

func (p *Page) IsLeaf() bool {
	return p.LeafNode != nil
}

func (p *Page) GetParent(pager *Pager) (*Page, error) {
	var parent int
	if p.LeafNode != nil {
		parent = p.LeafNode.Header.Parent
	} else {
		parent = p.NonLeafNode.Header.Parent
	}

	pp, err := pager.GetPage(parent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return pp, nil
}

func (p *Page) Marshal() (buf []byte, err error) {
	if p.IsLeaf() {
		if buf, err = p.LeafNode.Marshal(); err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		if buf, err = p.NonLeafNode.Marshal(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

type Pager struct {
	file     *os.File
	fileSize int64
	PageNum  int           // total page num
	cache    map[int]*Page // map[pageIdx]*Page
}

func PagerOpen(fileName string) (pager *Pager, err error) {
	var dbFile *os.File
	var fileLen int64

	if dbFile, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600); err != nil {
		return nil, errors.Trace(err)
	}

	// get file length
	if fileLen, err = dbFile.Seek(0, io.SeekEnd); err != nil {
		return nil, errors.Trace(err)
	}

	if fileLen%PageSize != 0 {
		return nil, fmt.Errorf("dbFile length must be n * PageSize")
	}

	pageNum := int(fileLen / PageSize)

	pager = &Pager{
		file:     dbFile,
		fileSize: fileLen,
		PageNum:  pageNum,
		cache:    make(map[int]*Page),
	}

	return
}

func (p *Pager) GetPage(pageIdx int) (page *Page, err error) {
	if pageIdx > p.PageNum {
		return nil, fmt.Errorf("out of bound")
	}

	if page = p.cache[pageIdx]; page != nil {
		return page, nil
	}

	// Cache miss
	// If pageIdx within data file, just read,
	// else just return blank page which will be flushed to db file later.
	buf := make([]byte, PageSize)
	// 从第几页开始读起,读取一页的内容
	if _, err = p.file.ReadAt(buf, int64(pageIdx*PageSize)); err != nil {
		if err != io.EOF {
			err = errors.Trace(err)
			return
		}
	}

	if buf[0] == 1 {
		node := &NonLeafNode{}
		if _, err := node.Unmarshal(buf); err != nil {
			return nil, errors.Trace(err)
		}
		page = &Page{NonLeafNode: node}
		page.NonLeafNode.Header.ID = pageIdx
	} else {
		node := &LeafNode{}
		if _, err := node.Unmarshal(buf); err != nil {
			return nil, errors.Trace(err)
		}
		page = &Page{LeafNode: node}
		page.LeafNode.Header.ID = pageIdx
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

	buf, err := page.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = p.file.WriteAt(buf, int64(pageIdx*PageSize))
	if err != nil {
		return errors.Trace(err)
	}
	return
}
