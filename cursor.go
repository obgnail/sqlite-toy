package sqlite

import (
	"github.com/juju/errors"
	"sync"
)

type Cursor struct {
	mutex   sync.RWMutex
	DB      *DB
	PageIdx int
	CellIdx int
}

func (cursor *Cursor) Insert(key int64, row any) (err error) {
	cursor.mutex.Lock()
	defer cursor.mutex.Unlock()

	page, err := cursor.DB.Pager.GetPage(cursor.PageIdx)
	if err != nil {
		return errors.Trace(err)
	}

	parent, err := cursor.DB.Pager.GetPage(page.LeafNode.Header.Parent)
	if err != nil {
		return errors.Trace(err)
	}

	cursor.setValue(parent, page, key, row)
	return
}

func (cursor *Cursor) setValue(parentPage *Page, curPage *Page, key int64, cell *Cell) {
	if curPage.IsLeaf() {
		curPage.LeafNode.SetValue(cell)
	}
}
