package sqlite

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
)

func TestBPT(t *testing.T) {
	bpt := NewBPTree(4, nil)

	bpt.Set(10, 1)
	bpt.Set(23, 1)
	bpt.Set(33, 1)
	bpt.Set(35, 1)
	bpt.Set(15, 1)
	bpt.Set(16, 1)
	bpt.Set(17, 1)
	bpt.Set(19, 1)

	bpt.Remove(23)

	if result := bpt.Get(10); !reflect.DeepEqual(result, 1) {
		t.Errorf("expected 1 and got %v \n", result)
	}
	if result := bpt.Get(15); !reflect.DeepEqual(result, 1) {
		t.Errorf("expected 1 and got %v \n", result)
	}
	if result := bpt.Get(20); result != nil {
		t.Errorf("expected nil and got %v \n", result)
	}

	if bpt.root.MaxKey != 35 {
		t.Errorf("maxKey err")
	}

	result := bpt.GetData()
	data, _ := json.MarshalIndent(result, "", "    ")
	t.Log(string(data))
}

func TestBPTRand(t *testing.T) {
	bpt := NewBPTree(3, nil)

	for i := 0; i < 12; i++ {
		key := rand.Int()%20 + 1
		t.Log(key)
		bpt.Set(int64(key), key)
	}

	data, _ := json.MarshalIndent(bpt.GetData(), "", "    ")
	t.Log(string(data))
}

func TestPosition(t *testing.T) {
	tree := NewBPTree(4, nil)
	for key := 1; key != 1000; key++ {
		tree.Set(int64(key), []byte("test"))
	}
	node1 := tree.root.findLeaf(1)
	if node1 == nil {
		t.Errorf("returned struct after delete \n")
	}
	for {
		if node1.Next == nil {
			break
		}
		node1 = node1.Next
	}
	item1 := node1.Items[len(node1.Items)-1]
	if item1.Key != 999 {
		t.Errorf("next err")
	}

	node2 := tree.root.findLeaf(999)
	if node2 == nil {
		t.Errorf("returned struct after delete \n")
	}
	for {
		if node2.Pre == nil {
			break
		}
		node2 = node2.Pre
	}
	item2 := node2.Items[0]
	if item2.Key != 1 {
		t.Errorf("pre err")
	}

	node := tree.GetFarRightLeaf()
	if node.Next != nil {
		t.Errorf("far right leaf node has next node")
	}
}

func TestInsertNilRoot(t *testing.T) {
	tree := NewBPTree(30, nil)

	var key int64 = 1
	value := []byte("test")

	tree.Set(key, value)

	result := tree.Get(key)
	if result == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(result, value) {
		t.Errorf("expected %v and got %v \n", value, result)
	}
}

func TestInsertSameKeyTwice(t *testing.T) {
	tree := NewBPTree(30, nil)

	var key int64 = 1
	value := []byte("test2222")

	tree.Set(key, value)
	tree.Set(key, value)

	result := tree.Get(key)
	if result == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(result, value) {
		t.Errorf("expected %v and got %v \n", string(value), string(result.([]byte)))
	}
}

func TestFindNilRoot(t *testing.T) {
	tree := NewBPTree(30, nil)

	result := tree.Get(1)

	if result != nil {
		t.Errorf("expected nil got %s \n", result)
	}
}

func TestDeleteNilTree(t *testing.T) {
	tree := NewBPTree(30, nil)

	var key int64 = 1

	tree.Remove(key)

	result := tree.Get(key)

	if result != nil {
		t.Errorf("expected nil got %s \n", result)
	}
}

func TestDelete(t *testing.T) {
	tree := NewBPTree(30, nil)

	var key int64 = 1
	value := []byte("test")

	tree.Set(key, value)
	result := tree.Get(key)
	if !reflect.DeepEqual(result, value) {
		t.Errorf("expected %v and got %v \n", string(value), string(result.([]byte)))
	}

	tree.Remove(key)

	result = tree.Get(key)
	if result != nil {
		t.Errorf("returned struct after delete \n")
	}
}

func TestUpdate(t *testing.T) {
	tree := NewBPTree(30, nil)

	var key int64 = 1
	value := []byte("test")

	update := tree.Set(key, value)
	if update == true {
		t.Errorf("expect false and got true")
	}

	update2 := tree.Set(key, []byte("test222"))
	if update2 == false {
		t.Errorf("expect true and got false")
	}

	update3 := tree.Set(key, []byte("test222"))
	if update3 == true {
		t.Errorf("expect false and got true")
	}
}

func TestDeleteNotFound(t *testing.T) {
	tree := NewBPTree(30, nil)

	var key int64 = 1
	value := []byte("test")

	tree.Set(key, value)
	result := tree.Get(key)
	if !reflect.DeepEqual(result, value) {
		t.Errorf("expected %v and got %v \n", string(value), string(result.([]byte)))
	}

	newKey := key + 1
	tree.Remove(newKey)

	result = tree.Get(newKey)
	if result != nil {
		t.Errorf("returned struct after delete \n")
	}
}

func TestMultiInsertAndDelete(t *testing.T) {
	tree := NewBPTree(3, nil)

	value := []byte("test")

	for key := 1; key != 1000; key++ {
		tree.Set(int64(key), value)
	}

	key := int64(999)
	ch := tree.GetAllItems()
	for item := range ch {
		if item.Key != key {
			t.Errorf("%d is not exist", key)
		}
		key--
	}

	if key != 0 {
		t.Errorf("%d is not exist", key)
	}

	if result := tree.Get(0); result != nil {
		t.Errorf("returned struct after delete \n")
	}
	if result := tree.Get(1); result == nil {
		t.Errorf("returned struct after delete \n")
	}
	if result := tree.Get(2); result == nil {
		t.Errorf("returned struct after delete \n")
	}
	if result := tree.Get(500); result == nil {
		t.Errorf("returned struct after delete \n")
	}
	if result := tree.Get(998); result == nil {
		t.Errorf("returned struct after delete \n")
	}
	if result := tree.Get(999); result == nil {
		t.Errorf("returned struct after delete \n")
	}
	if result := tree.Get(1000); result != nil {
		t.Errorf("returned struct after delete \n")
	}

	tree.Remove(1)
	if result := tree.Get(1); result != nil {
		t.Errorf("returned struct after delete \n")
	}
	tree.Remove(2)
	if result := tree.Get(2); result != nil {
		t.Errorf("returned struct after delete \n")
	}
	tree.Remove(-222222222)
	if result := tree.Get(-222222222); result != nil {
		t.Errorf("returned struct after delete \n")
	}
	tree.Remove(999)
	if result := tree.Get(999); result != nil {
		t.Errorf("returned struct after delete \n")
	}
	tree.Remove(1000)
	if result := tree.Get(1000); result != nil {
		t.Errorf("returned struct after delete \n")
	}
	tree.Remove(1001)
	if result := tree.Get(1001); result != nil {
		t.Errorf("returned struct after delete \n")
	}
}
