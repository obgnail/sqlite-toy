package sqlite

type Cursor struct {
	Table      *Table
	PageIdx    uint32
	CellIdx    uint32
	EndOfTable bool
}
