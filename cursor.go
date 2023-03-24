package sqlite

type Cursor struct {
	Table      *DB
	PageIdx    uint32
	CellIdx    uint32
	EndOfTable bool
}
