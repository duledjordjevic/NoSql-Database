package skiplist

import (
	"NAiSP/Structures/Record"
	"math/rand"
	"time"
)

const (
	NEG_INFINITY = ""
	POS_INFINITY = "■"
)

type Structure interface {
	Search(key string) *Node
	AddElement(record Record.Record) *Node
}

type Node struct {
	above *Node
	below *Node
	next  *Node
	prev  *Node
	key   string
	Value *Record.Record
}

func createNode(value *Record.Record) *Node {
	node := &Node{
		key:   value.GetKey(),
		Value: value,
		above: nil,
		below: nil,
		next:  nil,
		prev:  nil,
	}
	return node
}

type SkipList struct {
	head *Node
	tail *Node

	height uint
}

func createSkipList() *SkipList {
	skipList := &SkipList{}
	negRecord := Record.NewRecordKeyValue(NEG_INFINITY, []byte{1, 2}, byte(0))
	posRecord := Record.NewRecordKeyValue(POS_INFINITY, []byte{1, 2}, byte(0))
	skipList.head = createNode(negRecord)
	skipList.tail = createNode(posRecord)
	skipList.head.next = skipList.tail
	skipList.tail.prev = skipList.head

	skipList.height = 0
	return skipList
}

func (skipList *SkipList) Search(key string) *Node {
	n := skipList.head

	for n.below != nil {
		// fmt.Println("hello")
		n = n.below

		for key >= n.next.key {
			n = n.next
		}
	}

	return n

}

func (skipList *SkipList) AddElement(value *Record.Record) *Node {
	position := skipList.Search(value.GetKey())

	var q *Node

	var level int = -1
	var numberOfHeads int = -1

	if value.GetKey() == position.key {
		return position
	}

	for {
		numberOfHeads++
		level++

		skipList.canIncreaseLevel(level)

		q = position

		for position.above == nil {
			position = position.prev
		}

		position = position.above
		// fmt.Println(&position)
		q = skipList.insertAfterAbove(position, q, value)

		if rand.Seed(time.Now().UnixNano()); rand.Intn(2) == 0 {
			break
		}
	}
	return q
}

func (skipList *SkipList) canIncreaseLevel(level int) {
	if level >= int(skipList.height) {
		skipList.height += 1
		skipList.addEmptyLevel()
	}
}

func (skipList *SkipList) addEmptyLevel() {
	negRecord := Record.NewRecordKeyValue(NEG_INFINITY, []byte{1, 2}, byte(0))
	posRecord := Record.NewRecordKeyValue(POS_INFINITY, []byte{1, 2}, byte(0))
	newHeadNode := createNode(negRecord)
	newTailNode := createNode(posRecord)

	newHeadNode.next = newTailNode
	newHeadNode.below = skipList.head
	newTailNode.prev = newHeadNode
	newTailNode.below = skipList.tail

	skipList.head.above = newHeadNode
	skipList.tail.above = newTailNode

	// fmt.Println(&skipList.head)
	skipList.head = newHeadNode
	skipList.tail = newTailNode

}

func (skipList *SkipList) insertAfterAbove(position *Node, q *Node, value *Record.Record) *Node {

	newNode := createNode(value)
	// fmt.Println(position.below.key)

	nodeBeforeNewNode := position.below.below

	skipList.setBeforeAndAfterReferences(q, newNode)
	skipList.setAboveAndBelowReferences(position, value, newNode, nodeBeforeNewNode)

	return newNode
}

func (skipList *SkipList) setBeforeAndAfterReferences(q *Node, newNode *Node) {
	newNode.next = q.next
	newNode.prev = q
	q.next.prev = newNode
	q.next = newNode

}

func (skipList *SkipList) setAboveAndBelowReferences(position *Node, value *Record.Record, newNode *Node, nodeBeforeNewNode *Node) {
	if nodeBeforeNewNode != nil {
		for {
			if nodeBeforeNewNode.next.key != value.GetKey() {
				nodeBeforeNewNode = nodeBeforeNewNode.next
			} else {
				break
			}
		}
		newNode.below = nodeBeforeNewNode.next

		nodeBeforeNewNode.next.above = newNode
	}

	if position != nil {
		if position.next.key == value.GetKey() {
			newNode.above = position.next
		}
	}

}

// func (skipList *SkipList) removeElement(value) bool {
// 	nodeToBeRemoved := skipList.Search(key)

// 	if nodeToBeRemoved.key != key {
// 		return false
// 	}

// 	skipList.removeReferencesToNode(nodeToBeRemoved)

// 	for nodeToBeRemoved != nil {
// 		skipList.removeReferencesToNode(nodeToBeRemoved)

// 		if nodeToBeRemoved.above != nil {
// 			nodeToBeRemoved = nodeToBeRemoved.above
// 		} else {
// 			break
// 		}

// 	}
// 	return true
// }

// func (skipList *SkipList) removeReferencesToNode(nodeToBeRemoved *Node) {

// 	afterNodeToBeRemoved := nodeToBeRemoved.next
// 	beforeNodeToBeRemoved := nodeToBeRemoved.prev

// 	beforeNodeToBeRemoved.next = afterNodeToBeRemoved
// 	afterNodeToBeRemoved.prev = beforeNodeToBeRemoved

// }

func main() {
	// rand.Seed(time.Now().UnixNano())
	// fmt.Println(rand.Intn(2))

	skipList := createSkipList()

	// fmt.Println(skipList.head.key)
	negRecord := Record.NewRecordKeyValue("10", []byte{1, 2}, byte(0))
	// print(skipList.AddElement(negRecord))
	// skipList.AddElement(negRecord)
	// negRecord = record.NewRecordKeyValue(NEG_INFINITY, []byte{1, 2}, byte(0))
	skipList.AddElement(negRecord)
	negRecord = Record.NewRecordKeyValue("30", []byte{1, 2}, byte(0))
	skipList.AddElement(negRecord)
	negRecord = Record.NewRecordKeyValue("Dusan", []byte{1, 2}, byte(0))
	skipList.AddElement(negRecord)
	negRecord = Record.NewRecordKeyValue("Rade", []byte{1, 2}, byte(0))
	skipList.AddElement(negRecord)
	// skipList.AddElement(negRecord)
	print(skipList.Search("10").key)
	print(skipList.Search("30").key)
	print(skipList.Search("Rade").key)
	print(skipList.Search("Trajce").key)
	// fmt.Println(skipList.removeElement(10))

}
