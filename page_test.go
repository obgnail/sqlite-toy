package sqlite

import (
	"testing"
)

func TestPager(t *testing.T) {
	fileName := "./test.db"
	pager, err := PagerOpen(fileName)
	if err != nil {
		t.Fatal(err)
	}
	rootPageIdx := 0
	page, err := pager.GetPage(rootPageIdx)
	if err != nil {
		t.Fatal(err)
	}
}
