package bptree

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
)

func TestBPT(t *testing.T) {
	bpt := NewBPTree(4)

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
	bpt := NewBPTree(3)

	for i := 0; i < 12; i++ {
		key := rand.Int()%20 + 1
		t.Log(key)
		bpt.Set(int64(key), key)
	}

	data, _ := json.MarshalIndent(bpt.GetData(), "", "    ")
	t.Log(string(data))
}

func TestInsertNilRoot(t *testing.T) {
	tree := NewBPTree(30)

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
	tree := NewBPTree(30)

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
	tree := NewBPTree(30)

	result := tree.Get(1)

	if result != nil {
		t.Errorf("expected nil got %s \n", result)
	}
}

func TestDeleteNilTree(t *testing.T) {
	tree := NewBPTree(30)

	var key int64 = 1

	tree.Remove(key)

	result := tree.Get(key)

	if result != nil {
		t.Errorf("expected nil got %s \n", result)
	}
}

func TestDelete(t *testing.T) {
	tree := NewBPTree(30)

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

func TestDeleteNotFound(t *testing.T) {
	tree := NewBPTree(30)

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
	tree := NewBPTree(3)

	value := []byte("test")

	for key := 1; key != 1000; key++ {
		tree.Set(int64(key), value)
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
