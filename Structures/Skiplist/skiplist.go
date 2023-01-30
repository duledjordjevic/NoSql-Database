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

func CreateSkipList() *SkipList {
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

func (skipList *SkipList) AddElement(record *Record.Record) *Node {
	position := skipList.Search(record.GetKey())

	var q *Node

	var level int = -1
	var numberOfHeads int = -1

	if record.GetKey() == position.key {
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
		q = skipList.insertAfterAbove(position, q, record)

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

func (skip *SkipList) GetAllElements() []*Record.Record {
	listRecords := make([]*Record.Record, 0)

	n := skip.head
	for n.below != nil {
		n = n.below
	}
	n = n.next
	for n.key != skip.tail.key {
		listRecords = append(listRecords, n.Value)
		n = n.next
	}
	return listRecords
}
