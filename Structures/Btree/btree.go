package btree

import (
	"NAiSP/Structures/Record"
	"fmt"
)

type Structure interface {
	Search(key string) *Node
	AddElement(record *Record.Record) *Node
}

type Node struct {
	parent   *Node
	keys     []*Record.Record
	children []*Node
	isLeaf   bool
	n        int
	T        int
}

func createNode(T int) *Node {
	node := &Node{
		isLeaf: true,
		n:      0,
		parent: nil,
		T:      T,
	}

	node.keys = make([]*Record.Record, node.T-1)
	node.children = make([]*Node, node.T)

	for i := 0; i < node.T; i++ {
		if i != node.T-1 {
			node.keys[i] = nil
		}
		node.children[i] = nil
	}
	return node
}

type BTree struct {
	root *Node
	T    int
	n    int
}

func createBTree(T int) *BTree {
	bTree := &BTree{
		n:    0,
		root: nil,
		T:    T,
	}

	return bTree
}

func (bTree *BTree) Search(key string) *Node {
	if bTree.root == nil {
		return nil
	}
	currentNode := bTree.root
	tf := true
	for tf {
		indexChild := 0
		for i := 0; i < currentNode.n; i++ {
			if currentNode.keys[i].GetKey() == key {
				return currentNode
			}
			if key < currentNode.keys[i].GetKey() {
				break
			}
			indexChild++
		}
		if currentNode.isLeaf {
			break
		}
		currentNode = currentNode.children[indexChild]

	}

	return currentNode
}

func (bTree *BTree) sortKeys(record *Record.Record, position *Node) {
	index := -1
	for i := 0; i < position.n; i++ {
		if record.GetKey() < position.keys[i].GetKey() {
			index = i
			break
		}
	}
	if index == -1 {
		position.keys[position.n] = record
		position.n = position.n + 1
	} else {

		for index != position.n+1 {
			temp := position.keys[index]
			position.keys[index] = record
			// record = position.keys[index+1]
			record = temp
			index++
		}
		position.n = position.n + 1

	}
}

func (bTree *BTree) AddElement(record *Record.Record) *Node {
	position := bTree.Search(record.GetKey())

	//tree is empty
	if position == nil {
		rootNode := createNode(bTree.T)
		rootNode.keys[0] = record
		rootNode.n = 1
		bTree.root = rootNode
		return rootNode
	}
	//record already in tree
	for i := 0; i < position.n; i++ {
		if position.keys[i].GetKey() == record.GetKey() {
			return position
		}
	}

	//tree just have a root node
	if position == bTree.root {
		//root is full
		if position.T-1 == position.n {
			//adding record and sorting with that extra record
			position.keys = append(position.keys, nil)
			bTree.sortKeys(record, position)

			//newRoot
			newRootNode := createNode(bTree.T)
			newRootNode.keys[0] = position.keys[(position.n-1)/2]
			newRootNode.isLeaf = false
			newRootNode.n = 1
			position.keys[(position.n-1)/2] = nil

			//leftChild
			leftChildNode := createNode(bTree.T)
			for i := 0; i < position.n; i++ {
				if position.keys[i] == nil {
					break
				}
				leftChildNode.keys[i] = position.keys[i]
				leftChildNode.n += 1
			}
			//RightChild
			rightChildNode := createNode(bTree.T)
			index := 0
			for i := (position.n-1)/2 + 1; i < position.n; i++ {
				rightChildNode.keys[index] = position.keys[i]
				rightChildNode.n += 1
				index++
			}

			newRootNode.children[0] = leftChildNode
			newRootNode.children[1] = rightChildNode
			leftChildNode.parent = newRootNode
			rightChildNode.parent = newRootNode

			bTree.root = newRootNode

		} else {
			//root is not full
			bTree.sortKeys(record, position)

		}
	} else {
		//overflow
		if position.T-1 == position.n {

			position.keys = append(position.keys, nil)
			bTree.sortKeys(record, position)

			parentNode := position.parent
			overflowRecord := position.keys[(position.n-1)/2]

			position.keys[(position.n-1)/2] = nil
			//leftChild
			leftChildNode := createNode(bTree.T)
			for i := 0; i < position.n; i++ {
				if position.keys[i] == nil {
					break
				}
				leftChildNode.keys[i] = position.keys[i]
			}
			//RightChild
			rightChildNode := createNode(bTree.T)
			index := 0
			for i := (position.n-1)/2 + 1; i < position.n; i++ {
				rightChildNode.keys[index] = position.keys[i]
				index++
			}

			//promotion
			if parentNode.T-1 == parentNode.n+1 {

			} else {
				//parent go up
				bTree.sortKeys(overflowRecord, parentNode)
				newChildren := make([]*Node, bTree.T)
				for i := 0; i < bTree.T; i++ {
					newChildren[i] = nil
				}
				i := 0
				k := 0
				// is_append := false

				for i < len(parentNode.children) {
					if parentNode.children[i] == nil {
						break
					}
					if parentNode.children[i] != position {
						newChildren[k] = parentNode.children[i]
						k++
						i++
					} else {
						newChildren[k] = leftChildNode
						k++
						newChildren[k] = rightChildNode
						k++
						i++
					}
				}
				fmt.Println(newChildren)
				parentNode.children = newChildren

			}

		} else {
			bTree.sortKeys(record, position)

		}
	}
	return position
}

// func (bTree *BTree) print(record *record.Record) *Node {

// }

func main() {
	bTree := createBTree(4)
	record1 := Record.NewRecordKeyValue("123", []byte{100, 20}, 0)
	bTree.AddElement(record1)
	// keys := bTree.AddElement(record1).keys
	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }
	record2 := Record.NewRecordKeyValue("23", []byte{100, 20}, 0)
	bTree.AddElement(record2)
	// keys := bTree.AddElement(record2).keys

	record3 := Record.NewRecordKeyValue("456", []byte{100, 20}, 0)
	bTree.AddElement(record3)
	// keys := bTree.AddElement(record3).keys
	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }

	record4 := Record.NewRecordKeyValue("678", []byte{100, 20}, 0)
	bTree.AddElement(record4)

	// keys = bTree.AddElement(record4).keys

	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }

	record5 := Record.NewRecordKeyValue("534", []byte{100, 20}, 0)
	bTree.AddElement(record5)
	// keys = bTree.AddElement(record5).parent.keys
	// fmt.Println(bTree.AddElement(record5).parent.children[0].keys)
	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }

	record6 := Record.NewRecordKeyValue("537", []byte{100, 20}, 0)
	bTree.AddElement(record6)

	fmt.Println(bTree.root.children[2].keys)

}
