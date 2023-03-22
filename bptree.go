package bptree

import (
	"sync"
)

type BPItem struct {
	Key int64
	Val interface{}
}

type StringKV struct {
	Key string
	Val interface{}
}

type BPNode struct {
	MaxKey   int64     // 子树的最大关键字
	Children []*BPNode // 结点的子树
	Items    []BPItem  // 叶子结点的数据记录
	Next     *BPNode   // 叶子结点中指向下一个叶子结点，用于实现叶子结点链表
}

func search(len int, target int64, f func(i int) int64) (int, bool) {
	low, high := 0, len-1
	for low <= high {
		var mid = low + (high-low)/2
		v := f(mid)
		if v == target {
			return mid, true
		} else if v > target {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return low, false
}

func (node *BPNode) findItem(key int64) (int, bool) {
	return search(len(node.Items), key, func(i int) int64 { return node.Items[i].Key })
}

func (node *BPNode) findChild(key int64) (int, bool) {
	return search(len(node.Children), key, func(i int) int64 { return node.Children[i].MaxKey })
}

func (node *BPNode) findLeaf(key int64) *BPNode {
	n := node
	for i := 0; i < len(n.Children); i++ {
		if key <= n.Children[i].MaxKey {
			n = n.Children[i]
			i = 0
		}
	}

	// 没有到达叶子结点
	if len(n.Children) > 0 {
		return nil
	}

	return n
}

func (node *BPNode) setValue(key int64, value interface{}) {
	item := BPItem{key, value}
	num := len(node.Items)
	if num == 0 {
		node.Items = append(node.Items, item)
		node.MaxKey = item.Key
		return
	} else if key < node.Items[0].Key {
		node.Items = append([]BPItem{item}, node.Items...)
		return
	} else if key > node.Items[num-1].Key {
		node.Items = append(node.Items, item)
		node.MaxKey = item.Key
		return
	}

	idx, exist := node.findItem(key)
	if !exist {
		node.Items = append(node.Items, BPItem{})
		copy(node.Items[idx+1:], node.Items[idx:])
	}
	node.Items[idx] = item
}

func (node *BPNode) addItem(item ...BPItem) {
	for _, i := range item {
		node.setValue(i.Key, i.Val)
	}
}

func (node *BPNode) deleteItem(key int64) bool {
	idx, exist := node.findItem(key)
	if !exist {
		return false
	}

	copy(node.Items[idx:], node.Items[idx+1:])
	node.Items = node.Items[0 : len(node.Items)-1]
	node.MaxKey = node.Items[len(node.Items)-1].Key
	return true
}

func (node *BPNode) popLastItem() BPItem {
	last := len(node.Items) - 1
	item := node.Items[last]
	node.Items = node.Items[:last]
	node.MaxKey = node.Items[last].Key
	return item
}

func (node *BPNode) popFirstItem() BPItem {
	item := node.Items[0]
	node.Items = node.Items[1:]
	node.MaxKey = node.Items[len(node.Items)-1].Key // 有可能只有一个,pop出去后就没有了
	return item
}

func (node *BPNode) popLastChild() *BPNode {
	last := len(node.Children) - 1
	child := node.Children[last]
	node.Children = node.Children[:last]
	node.MaxKey = node.Children[last].MaxKey
	return child
}

func (node *BPNode) popFirstChild() *BPNode {
	child := node.Children[0]
	node.Children = node.Children[1:]
	node.MaxKey = node.Children[len(node.Children)-1].MaxKey // 有可能只有一个,pop出去后就没有了
	return child
}

func (node *BPNode) addChild(child *BPNode) {
	num := len(node.Children)
	if num == 0 {
		node.Children = append(node.Children, child)
		node.MaxKey = child.MaxKey
		return
	} else if child.MaxKey < node.Children[0].MaxKey {
		node.Children = append([]*BPNode{child}, node.Children...)
		return
	} else if child.MaxKey > node.Children[num-1].MaxKey {
		node.Children = append(node.Children, child)
		node.MaxKey = child.MaxKey
		return
	}

	idx, _ := node.findChild(child.MaxKey)
	node.Children = append(node.Children, nil)
	copy(node.Children[idx+1:], node.Children[idx:])
	node.Children[idx] = child
}

func (node *BPNode) addChildren(children []*BPNode) {
	for _, child := range children {
		node.addChild(child)
	}
}

func (node *BPNode) deleteChild(child *BPNode) bool {
	idx, exist := node.findChild(child.MaxKey)
	if !exist {
		return false
	}
	copy(node.Children[idx:], node.Children[idx+1:])
	node.Children = node.Children[0 : len(node.Children)-1]
	node.MaxKey = node.Children[len(node.Children)-1].MaxKey
	return true
}

type BPTree struct {
	mutex     sync.RWMutex
	root      *BPNode
	width     int // B+树的阶
	halfWidth int // ceil(M/2)
}

func NewBPTree(width int) *BPTree {
	if width < 3 {
		width = 3
	}

	var bt = &BPTree{}
	bt.root = NewLeafNode(width)
	bt.width = width
	bt.halfWidth = (bt.width + 1) / 2
	return bt
}

func NewLeafNode(width int) *BPNode {
	var node = &BPNode{}
	// 申请width+1是因为插入时可能暂时出现节点key大于申请width的情况,待后期再分裂处理
	node.Items = make([]BPItem, width+1)
	node.Items = node.Items[0:0]
	return node
}

func NewIndexNode(width int) *BPNode {
	var node = &BPNode{}
	// 申请width+1是因为插入时可能暂时出现节点key大于申请width的情况,待后期再分裂处理
	node.Children = make([]*BPNode, width+1)
	node.Children = node.Children[0:0]
	return node
}

func (t *BPTree) Get(key int64) interface{} {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	node := t.root.findLeaf(key)
	if node == nil {
		return nil
	}

	idx, exist := node.findItem(key)
	if !exist {
		return nil
	}
	res := node.Items[idx].Val
	return res
}

func (t *BPTree) GetData() map[int64]interface{} {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.getData(t.root)
}

func (t *BPTree) getData(node *BPNode) map[int64]interface{} {
	data := make(map[int64]interface{})
	for {
		// 非叶子节点
		if len(node.Children) > 0 {
			for i := 0; i < len(node.Children); i++ {
				data[node.Children[i].MaxKey] = t.getData(node.Children[i])
			}
			break
			// 叶子节点
		} else {
			for i := 0; i < len(node.Items); i++ {
				data[node.Items[i].Key] = node.Items[i].Val
			}
			break
		}
	}
	return data
}

func (t *BPTree) splitNode(node *BPNode) (newNode *BPNode) {
	// 子节点数大于t.width
	if len(node.Children) > t.width {
		halfW := t.width/2 + 1

		//创建新结点
		newNode = NewIndexNode(t.width)
		newNode.addChildren(node.Children[halfW:len(node.Children)])

		//修改原结点数据
		node.Children = node.Children[0:halfW]
		node.MaxKey = node.Children[len(node.Children)-1].MaxKey

		return newNode

		// 记录数大于t.width
	} else if len(node.Items) > t.width {
		halfW := t.width/2 + 1

		//创建新结点
		newNode = NewLeafNode(t.width)
		newNode.addItem(node.Items[halfW:len(node.Items)]...)

		//修改原结点数据
		node.Next = newNode
		node.Items = node.Items[0:halfW]
		node.MaxKey = node.Items[len(node.Items)-1].Key

		return newNode
	}

	return nil
}

func (t *BPTree) setValue(parent *BPNode, node *BPNode, key int64, value interface{}) {
	for i := 0; i < len(node.Children); i++ {
		if key <= node.Children[i].MaxKey || i == len(node.Children)-1 {
			t.setValue(node, node.Children[i], key, value)
			break
		}
	}

	//叶子结点，添加数据
	if len(node.Children) == 0 {
		node.setValue(key, value)
	}

	// 结点分裂
	nodeNew := t.splitNode(node)
	if nodeNew == nil {
		return
	}

	//若父结点不存在，则创建一个父节点
	if parent == nil {
		parent = NewIndexNode(t.width)
		parent.addChild(node)
		t.root = parent
	}
	//添加结点到父亲结点
	parent.addChild(nodeNew)
}

func (t *BPTree) Set(key int64, value interface{}) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.setValue(nil, t.root, key, value)
}

func (t *BPTree) itemMoveOrMerge(parent *BPNode, curNode *BPNode) {
	//获取兄弟结点
	var preNode *BPNode
	var nextNode *BPNode
	for i := 0; i < len(parent.Children); i++ {
		if parent.Children[i] == curNode {
			if i < len(parent.Children)-1 {
				nextNode = parent.Children[i+1]
			} else if i > 0 {
				preNode = parent.Children[i-1]
			}
			break
		}
	}

	//将左侧结点的记录移动到删除结点
	if preNode != nil && len(preNode.Items) > t.halfWidth {
		item := preNode.popLastItem()
		curNode.addItem(item)
		return
	}

	//将右侧结点的记录移动到删除结点
	if nextNode != nil && len(nextNode.Items) > t.halfWidth {
		item := nextNode.popFirstItem()
		curNode.addItem(item)
		return
	}

	//与左侧结点进行合并
	if preNode != nil && len(preNode.Items)+len(curNode.Items) <= t.width {
		preNode.addItem(curNode.Items...)
		preNode.Next = curNode.Next
		parent.deleteChild(curNode)
		return
	}

	//与右侧结点进行合并
	if nextNode != nil && len(nextNode.Items)+len(curNode.Items) <= t.width {
		curNode.addItem(nextNode.Items...)
		curNode.Next = nextNode.Next
		parent.deleteChild(nextNode)
		return
	}
}

func (t *BPTree) childMoveOrMerge(parent *BPNode, curNode *BPNode) {
	if parent == nil {
		return
	}

	//获取兄弟结点
	var preNode *BPNode
	var nextNode *BPNode
	for i := 0; i < len(parent.Children); i++ {
		if parent.Children[i] == curNode {
			if i < len(parent.Children)-1 {
				nextNode = parent.Children[i+1]
			} else if i > 0 {
				preNode = parent.Children[i-1]
			}
			break
		}
	}

	//将左侧结点的子结点移动到删除结点
	if preNode != nil && len(preNode.Children) > t.halfWidth {
		child := preNode.popLastChild()
		curNode.addChild(child)
		return
	}

	//将右侧结点的子结点移动到删除结点
	if nextNode != nil && len(nextNode.Children) > t.halfWidth {
		child := nextNode.popFirstChild()
		curNode.addChild(child)
		return
	}

	//与左侧结点进行合并
	if preNode != nil && len(preNode.Children)+len(curNode.Children) <= t.width {
		preNode.addChildren(curNode.Children)
		parent.deleteChild(curNode)
		return
	}

	//与右侧结点进行合并
	if nextNode != nil && len(nextNode.Children)+len(curNode.Children) <= t.width {
		curNode.addChildren(nextNode.Children)
		parent.deleteChild(nextNode)
		return
	}
}

func (t *BPTree) deleteItem(parent *BPNode, node *BPNode, key int64) {
	for i := 0; i < len(node.Children); i++ {
		if key <= node.Children[i].MaxKey {
			t.deleteItem(node, node.Children[i], key)
			break
		}
	}

	// 叶子节点
	if len(node.Children) == 0 {
		//删除记录后若结点的子项 小于 m/2，则从兄弟结点移动记录，或者合并结点
		node.deleteItem(key)
		if len(node.Items) < t.halfWidth {
			t.itemMoveOrMerge(parent, node)
		}
		// 非叶子节点
	} else {
		//若结点的子项 小于 m/2，则从兄弟结点移动记录，或者合并结点
		node.MaxKey = node.Children[len(node.Children)-1].MaxKey // 维护祖先节点的maxKey
		if len(node.Children) < t.halfWidth {
			t.childMoveOrMerge(parent, node)
		}
	}
}

func (t *BPTree) Remove(key int64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.deleteItem(nil, t.root, key)
}
