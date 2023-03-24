package sqlite

import "github.com/juju/errors"

type DB struct {
	Pager       *Pager
	RootPageIdx int
}

func Open(fileName string) (db *DB, err error) {
	pager, err := PagerOpen(fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db = &DB{Pager: pager, RootPageIdx: 0}

	var p *Page
	if pager.PageNum == 0 {
		// New database file, initialize page 0 as leaf node.
		if p, err = db.Pager.GetPage(0); err != nil {
			return
		}
		p.LeafNode.Header.IsNonLeaf = false
		p.LeafNode.Header.IsDeleted = false
		p.LeafNode.Header.IsRoot = true
		p.LeafNode.Header.ID = db.RootPageIdx
	}

	return
}
