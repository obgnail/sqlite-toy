package sqlite

import (
	"encoding/binary"
	"github.com/juju/errors"
)

type NonLeafNode struct {
	Header   *Header
	Children []*Child
}

func (node *NonLeafNode) GetMaxKey() int64 {
	if len(node.Children) == 0 {
		return 0
	}
	key := node.Children[len(node.Children)-1].MaxKey
	return key
}

func (node *NonLeafNode) IsLeaf() bool { return false }

func (node *NonLeafNode) Size() int64 {
	res := node.Header.Size()
	for i := range node.Children {
		res += node.Children[i].Size()
	}
	return res
}

func (node *NonLeafNode) Marshal() ([]byte, error) {
	size := node.Size()
	res := make([]byte, 0, size)

	_head, err := node.Header.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	res = append(res, _head...)

	for _, child := range node.Children {
		_child, _err := child.Marshal()
		if _err != nil {
			return nil, errors.Trace(_err)
		}
		res = append(res, _child...)
	}
	return res, nil
}

func (node *NonLeafNode) Unmarshal(buf []byte) (res int64, err error) {
	node.Header = new(Header)
	count, err := node.Header.Unmarshal(buf)
	if err != nil {
		return 0, errors.Trace(err)
	}
	res += count

	node.Header.IsNonLeaf = true

	for i := 0; i != node.Header.ChildNum; i++ {
		c := &Child{}
		_count, _err := c.Unmarshal(buf[res:])
		if _err != nil {
			return 0, errors.Trace(_err)
		}
		node.Children = append(node.Children, c)
		res += _count
	}
	return res, nil
}

type LeafNode struct {
	Header   *Header
	PreNode  int
	NextNode int
	Cells    []*Cell
}

func (node *LeafNode) GetMaxKey() int64 {
	if len(node.Cells) == 0 {
		return 0
	}
	key := node.Cells[len(node.Cells)-1].Key
	return key
}

func (node *LeafNode) IsLeaf() bool { return true }

func (node *LeafNode) Size() int64 {
	res := node.Header.Size()
	res += 8
	res += int64(node.Header.ChildNum * node.Header.ChildSize)
	return res
}

func (node *LeafNode) Marshal() ([]byte, error) {
	size := node.Size()
	res := make([]byte, 0, size)

	_head, err := node.Header.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	res = append(res, _head...)

	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b[:4], uint32(node.PreNode))
	binary.LittleEndian.PutUint32(b[4:], uint32(node.NextNode))
	res = append(res, b...)

	for _, cell := range node.Cells {
		_cell, _err := cell.Marshal()
		if _err != nil {
			return nil, errors.Trace(_err)
		}
		res = append(res, _cell...)
	}

	return res, nil
}

func (node *LeafNode) Unmarshal(buf []byte) (res int64, err error) {
	node.Header = new(Header)
	count, err := node.Header.Unmarshal(buf)
	if err != nil {
		return 0, errors.Trace(err)
	}
	res += count

	node.Header.IsNonLeaf = false

	for i := 0; i != node.Header.ChildNum; i++ {
		c := &Cell{}
		_count, _err := c.Unmarshal(buf[res : res+int64(node.Header.ChildSize)])
		if _err != nil {
			return 0, errors.Trace(_err)
		}
		node.Cells = append(node.Cells, c)
		res += _count
	}
	return res, nil
}

func (node *LeafNode) SetValue(cell *Cell) {
	idx, exist := findCell(node.Cells, cell.Key)
	if !exist {
		node.Cells = append(node.Cells, &Cell{})
		copy(node.Cells[idx+1:], node.Cells[idx:])
	}
	node.Cells[idx] = cell
}

type Header struct {
	IsNonLeaf bool
	IsDeleted bool
	IsRoot    bool
	ID        int
	Parent    int
	ChildNum  int
	ChildSize int
}

func (h *Header) Size() int64 { return 19 }

func (h *Header) Marshal() ([]byte, error) {
	size := h.Size()
	buf := make([]byte, size)

	if h.IsNonLeaf {
		buf[0] = 1 // leaf
	} else {
		buf[0] = 0 // non-leaf
	}

	if h.IsDeleted {
		buf[1] = 1
	} else {
		buf[1] = 0
	}

	if h.IsRoot {
		buf[2] = 1
	} else {
		buf[2] = 0
	}

	binary.LittleEndian.PutUint32(buf[3:], uint32(h.ID))
	binary.LittleEndian.PutUint32(buf[7:], uint32(h.Parent))
	binary.LittleEndian.PutUint32(buf[11:], uint32(h.ChildNum))
	binary.LittleEndian.PutUint32(buf[15:], uint32(h.ChildSize))
	return buf, nil
}

func (h *Header) Unmarshal(buf []byte) (int64, error) {
	h.IsNonLeaf = buf[0] == 1
	h.IsDeleted = buf[1] == 1
	h.IsRoot = buf[2] == 1
	h.ID = int(binary.LittleEndian.Uint32(buf[3:]))
	h.Parent = int(binary.LittleEndian.Uint32(buf[7:]))
	h.ChildNum = int(binary.LittleEndian.Uint32(buf[11:]))
	h.ChildSize = int(binary.LittleEndian.Uint32(buf[15:]))
	return h.Size(), nil
}

type Child struct {
	ID     int
	MaxKey int64
}

func (c *Child) Size() int64 { return 8 }

func (c *Child) Marshal() ([]byte, error) {
	size := c.Size()
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf, uint32(c.ID))
	binary.LittleEndian.PutUint32(buf[4:], uint32(c.MaxKey))
	return buf, nil
}

func (c *Child) Unmarshal(buf []byte) (int64, error) {
	c.ID = int(binary.LittleEndian.Uint32(buf))
	c.MaxKey = int64(binary.LittleEndian.Uint32(buf[4:]))
	return c.Size(), nil
}

type Cell struct {
	Key   int64
	Value []byte
}

func (c *Cell) Marshal() ([]byte, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(c.Key))
	buf = append(buf, c.Value...)
	return buf, nil
}

func (c *Cell) Unmarshal(buf []byte) (int64, error) {
	c.Key = int64(binary.LittleEndian.Uint32(buf))
	c.Value = buf[4:]
	return int64(len(buf)), nil
}
