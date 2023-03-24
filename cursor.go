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

	cursor.setValue(page, key, row)
	return
}

func setValue(pager *Pager, curPage *Page, key int64, cell *Cell) error {
	if curPage.IsLeaf() {
		curPage.LeafNode.SetValue(cell)
	} else {
		curPage.NonLeafNode.
	}

	parent, err := curPage.GetParent(pager)
	if err != nil {
		return errors.Trace(err)
	}
	return setValue(pager, parent, key, cell)
}
