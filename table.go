package sqlite

type Table struct {
	Pager       *Pager
	RootPageIdx uint32
}
