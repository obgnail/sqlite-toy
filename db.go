package sqlite

//import (
//	"fmt"
//	"github.com/juju/errors"
//)
//
//type DB struct {
//	Pager       *Pager
//	RootPageIdx int
//}
//
//func Open(fileName string) (db *DB, err error) {
//	pager, err := PagerOpen(fileName)
//	if err != nil {
//		return nil, errors.Trace(err)
//	}
//
//	db = &DB{Pager: pager, RootPageIdx: 0}
//
//	var p *Page
//	if pager.PageNum == 0 {
//		// New database file, initialize page 0 as leaf node.
//		if p, err = db.Pager.GetPage(0); err != nil {
//			return
//		}
//		p.LeafNode.Header.IsNonLeaf = false
//		p.LeafNode.Header.IsDeleted = false
//		p.LeafNode.Header.IsRoot = true
//		p.LeafNode.Header.ID = db.RootPageIdx
//	}
//
//	return
//}
//
//func (db *DB) SeekCursor(key int64) (*Cursor, bool, error) {
//	rootPage, err := db.Pager.GetPage(db.RootPageIdx)
//	if err != nil {
//		return nil, false, errors.Trace(err)
//	}
//
//	seekFunc := db.SeekNonLeaf
//	if rootPage.IsLeaf() {
//		seekFunc = db.SeekLeaf
//	}
//
//	cur, exist, err := seekFunc(db.RootPageIdx, key)
//	if err != nil {
//		return nil, false, errors.Trace(err)
//	}
//	return cur, exist, nil
//}
//
//// SeekLeaf: 根据key,找到在Node中第几个cell
//func (db *DB) SeekLeaf(pageIdx int, key int64) (cursor *Cursor, exist bool, err error) {
//	page, err := db.Pager.GetPage(pageIdx)
//	if err != nil {
//		return nil, false, errors.Trace(err)
//	}
//
//	// If cellIdx is not exist, it is the insert location, otherwise it is the update location
//	cellIdx, exist := findCell(page.LeafNode.Cells, key)
//
//	cursor = &Cursor{
//		DB:      db,
//		PageIdx: pageIdx,
//		CellIdx: cellIdx,
//	}
//	return cursor, exist, nil
//}
//
//func (db *DB) SeekNonLeaf(pageIdx int, key int64) (cursor *Cursor, exist bool, err error) {
//	page, err := db.Pager.GetPage(pageIdx)
//	if err != nil {
//		return nil, false, errors.Trace(err)
//	}
//
//	if page.IsLeaf() {
//		return db.SeekLeaf(pageIdx, key)
//	}
//
//	childIdx, _ := findChild(page.NonLeafNode.Children, key)
//
//	var nextPage int
//	if childIdx == len(page.NonLeafNode.Children) {
//		nextPage = page.NonLeafNode.Children[childIdx-1].ID
//	} else {
//		nextPage = page.NonLeafNode.Children[childIdx].ID
//	}
//	return db.SeekNonLeaf(nextPage, key)
//}
//
//func (db *DB) Insert(key int64, row interface{}) (err error) {
//	cursor, exist, err := db.SeekCursor(key)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	if exist {
//		return fmt.Errorf("duplicate key %d", key)
//	}
//
//	page, err := db.Pager.GetPage(cursor.PageIdx)
//	if err != nil {
//		return errors.Trace(err)
//	}
//
//	// Must be leaf node
//	if page.LeafNode == nil {
//		panic("should be leaf node")
//	}
//
//	return cursor.Insert(key, row)
//}
//
//func findCell(cells []*Cell, key int64) (int, bool) {
//	return search(len(cells), key, func(i int) int64 { return cells[i].Key })
//}
//
//func findChild(children []*Child, key int64) (int, bool) {
//	return search(len(children), key, func(i int) int64 { return children[i].MaxKey })
//}
