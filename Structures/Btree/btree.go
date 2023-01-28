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
	Keys     []*Record.Record
	Children []*Node
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

	node.Keys = make([]*Record.Record, node.T-1)
	node.Children = make([]*Node, node.T)

	for i := 0; i < node.T; i++ {
		if i != node.T-1 {
			node.Keys[i] = nil
		}
		node.Children[i] = nil
	}
	return node
}

type BTree struct {
	Root *Node
	T    int
	n    int
}

func CreateBTree(T int) *BTree {
	bTree := &BTree{
		n:    0,
		Root: nil,
		T:    T,
	}

	return bTree
}

func (bTree *BTree) Search(key string) *Node {
	if bTree.Root == nil {
		return nil
	}
	currentNode := bTree.Root
	tf := true
	for tf {
		indexChild := 0
		for i := 0; i < currentNode.n; i++ {
			if currentNode.Keys[i].GetKey() == key {
				return currentNode
			}
			if key < currentNode.Keys[i].GetKey() {
				break
			}
			indexChild++
		}
		if currentNode.isLeaf {
			break
		}
		currentNode = currentNode.Children[indexChild]

	}

	return currentNode
}

func (bTree *BTree) sortKeys(record *Record.Record, position *Node) {
	index := -1
	for i := 0; i < position.n; i++ {
		if record.GetKey() < position.Keys[i].GetKey() {
			index = i
			break
		}
	}
	if index == -1 {
		position.Keys[position.n] = record
		position.n = position.n + 1
	} else {

		for index != position.n+1 {
			temp := position.Keys[index]
			position.Keys[index] = record
			// record = position.Keys[index+1]
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
		RootNode := createNode(bTree.T)
		RootNode.Keys[0] = record
		RootNode.n = 1
		bTree.Root = RootNode
		return RootNode
	}
	//record already in tree
	for i := 0; i < position.n; i++ {
		if position.Keys[i].GetKey() == record.GetKey() {
			return position
		}
	}

	for true {
		//tree just have a Root node
		if position == bTree.Root && position.Children[0] == nil {
			//Root is full
			if position.T-1 == position.n {
				//adding record and sorting with that extra record
				position.Keys = append(position.Keys, nil)
				bTree.sortKeys(record, position)

				//newRoot
				newRootNode := createNode(bTree.T)
				newRootNode.Keys[0] = position.Keys[(position.n-1)/2]
				newRootNode.isLeaf = false
				newRootNode.n = 1
				position.Keys[(position.n-1)/2] = nil

				//leftChild
				leftChildNode := createNode(bTree.T)
				for i := 0; i < position.n; i++ {
					if position.Keys[i] == nil {
						break
					}
					leftChildNode.Keys[i] = position.Keys[i]
					leftChildNode.n += 1
				}
				//RightChild
				rightChildNode := createNode(bTree.T)
				index := 0
				for i := (position.n-1)/2 + 1; i < position.n; i++ {
					rightChildNode.Keys[index] = position.Keys[i]
					rightChildNode.n += 1
					index++
				}

				newRootNode.Children[0] = leftChildNode
				newRootNode.Children[1] = rightChildNode
				leftChildNode.parent = newRootNode
				rightChildNode.parent = newRootNode

				bTree.Root = newRootNode
				break
			} else {
				//Root is not full
				bTree.sortKeys(record, position)
				break
			}
		} else {
			//overflow
			// fmt.Println(position.T, position.n, "hello")

			if position.T-1 == position.n {
				position.Keys = append(position.Keys, nil)
				bTree.sortKeys(record, position)

				parentNode := position.parent
				overflowRecord := position.Keys[(position.n-1)/2]

				position.Keys[(position.n-1)/2] = nil

				//promotion
				// fmt.Println(parentNode.T, parentNode.n+1)
				// fmt.Println(parentNode.Keys)
				// fmt.Println(position.Keys, position.parent)
				if parentNode.T == parentNode.n+1 {
					if parentNode == bTree.Root {
						newRootNode := createNode(bTree.T)
						newRootNode.n = 1
						newRootNode.Keys[0] = overflowRecord

						keysBeforeOverflow := make([]*Record.Record, bTree.T-1)
						keysAfterOverflow := make([]*Record.Record, bTree.T-1)
						childrenBeforeOverflow := make([]*Node, bTree.T)
						childrenAfterOverflow := make([]*Node, bTree.T)
						for i := 0; i < bTree.T; i++ {
							if i != bTree.T-1 {
								keysBeforeOverflow[i] = nil
								keysAfterOverflow[i] = nil
							}
							childrenBeforeOverflow[i] = nil
							childrenAfterOverflow[i] = nil
						}
						index := (position.n - 1) / 2
						k1 := 0
						k2 := 0
						for i := 0; i < position.n-1; i++ {
							if i < index {
								keysBeforeOverflow[k1] = position.Keys[i]
								k1++
							} else if i > index {
								keysAfterOverflow[k2] = position.Keys[i]
								k2++
							}
						}
						nodeBefore := createNode(bTree.T)
						nodeAfter := createNode(bTree.T)
						nodeBefore.Keys = keysBeforeOverflow
						nodeBefore.n = k1
						nodeAfter.Keys = keysAfterOverflow
						nodeAfter.n = k2

						k1, k2 = 0, 0
						fmt.Println(position.n+1, index)
						for i := 0; i < position.n; i++ {
							if i <= index {
								childrenBeforeOverflow[k1] = position.Children[i]
								k1++
							} else {
								childrenAfterOverflow[k2] = position.Children[i]
								print(k2)
								k2++
							}
						}
						nodeBefore.Children = childrenBeforeOverflow
						nodeAfter.Children = childrenAfterOverflow

						nodeBefore.parent, nodeAfter.parent = newRootNode, newRootNode
						newRootNode.Children[0] = nodeBefore
						newRootNode.Children[1] = nodeAfter

						bTree.Root = newRootNode
						break

					} else {
						childKeysToMove := make([]*Record.Record, bTree.T-1)
						childrenToMove := make([]*Node, bTree.T)
						newChildren := make([]*Node, bTree.T)
						for i := 0; i < bTree.T; i++ {
							if i != bTree.T-1 {
								childKeysToMove[i] = nil
							}
							childrenToMove[i] = nil
							newChildren[i] = nil
						}

						//childKeysToMove
						k := 0
						for i := (position.n - 1) / 2; i < position.n; i++ {
							childKeysToMove[k] = position.Keys[i]
							position.Keys[i] = nil
							k++
						}
						nodeToMove := createNode(bTree.T)
						nodeToMove.Keys = childKeysToMove

						nodeToMove.n = k

						//childrenToMove
						k = 0
						for i := ((position.n - 1) / 2) + 1; i < position.T; i++ {
							childrenToMove[k] = position.Children[i]
							k++
						}

						nodeToMove.Children = childrenToMove
						bTree.sortKeys(overflowRecord, parentNode)

						index := (position.n-1)/2 + 1

						//newChildren
						k = 0
						for i := 0; i < parentNode.n+1; i++ {
							if k == index {
								newChildren[k] = nodeToMove
								k++
							}
							newChildren[k] = parentNode.Children[i]
							k++
						}
						parentNode.Children = newChildren

						position = parentNode
					}

				} else {
					//parent go up

					//leftChild
					leftChildNode := createNode(bTree.T)
					for i := 0; i < position.n; i++ {
						if position.Keys[i] == nil {
							break
						}
						leftChildNode.Keys[i] = position.Keys[i]
						leftChildNode.n += 1
					}
					//RightChild
					rightChildNode := createNode(bTree.T)
					index := 0
					for i := (position.n-1)/2 + 1; i < position.n; i++ {
						rightChildNode.Keys[index] = position.Keys[i]
						rightChildNode.n += 1
						index++
					}

					bTree.sortKeys(overflowRecord, parentNode)
					newChildren := make([]*Node, bTree.T)
					for i := 0; i < bTree.T; i++ {
						newChildren[i] = nil
					}
					i := 0
					k := 0
					// is_append := false

					for i < len(parentNode.Children) {
						if parentNode.Children[i] == nil {
							break
						}
						if parentNode.Children[i] != position {
							newChildren[k] = parentNode.Children[i]
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
					// fmt.Println(newChildren)
					leftChildNode.parent = parentNode
					rightChildNode.parent = parentNode
					parentNode.Children = newChildren
					break
				}

			} else {
				bTree.sortKeys(record, position)
				break
			}
		}
	}
	return position
}

func (bTree *BTree) Print(root *Node) {
	if root != nil {
		for _, element := range root.Keys {
			if element != nil {
				fmt.Print(element.GetKey() + "  ")
			}
		}
		fmt.Println("")
		if root.Children[0] != nil {
			for _, element := range root.Children {
				bTree.Print(element)
			}
		}
	}

}

// func main() {

// }
