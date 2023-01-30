package Memtable

import (
	btree "NAiSP/Structures/Btree"
	record "NAiSP/Structures/Record"
	skiplist "NAiSP/Structures/Skiplist"
)

type MemTable struct {
	Capacity         float64
	Trashold         float64
	StructName       string
	numberOFElements float64
	skipList         *skiplist.SkipList
	bTree            *btree.BTree
}

func (mem *MemTable) FillDefaults() {

	if mem.Capacity == 0 {
		mem.Capacity = 100
	}

	if mem.Trashold == 0 {
		mem.Trashold = 0.8
	}

	if mem.numberOFElements != 0 {
		mem.numberOFElements = 0
	}

	if mem.StructName == "btree" {
		mem.bTree = btree.CreateBTree(4)
	} else {
		mem.skipList = skiplist.CreateSkipList()
	}
}

func (mem *MemTable) Empty() {
	mem.FillDefaults()
}

func (mem *MemTable) Find(key string) bool {
	if mem.StructName == "btree" {
		found := mem.bTree.Search(key)

		for _, i := range found.Keys {
			if i.GetKey() == key {
				if i.GetTombStone() == 1 {
					return false
				}
				return true
			}
		}

	} else {

		found := mem.skipList.Search(key)
		if found.Value.GetKey() == key {
			if found.Value.GetTombStone() == 1 {
				return false
			} else {
				return true
			}
		}

	}
	return false
}

func (mem *MemTable) Add(record *record.Record) *[]*record.Record {

	if mem.StructName == "btree" {
		elements, found := mem.bTree.AddElement(record)
		if found {
			for i := range elements.Keys {
				if elements.Keys[i].GetKey() == record.GetKey() {
					elements.Keys[i] = record
				}
			}
		} else {
			mem.numberOFElements++
			if mem.numberOFElements/mem.Capacity >= mem.Trashold {
				return mem.Flush()
			}
		}
	} else {

		found := mem.skipList.AddElement(record)
		if found.Value.GetKey() == record.GetKey() && found.Value.GetTimeStamp() == record.GetTimeStamp() && found.Value.GetTombStone() == record.GetTombStone() && string(found.Value.GetValue()) == string(record.GetValue()) {
			mem.numberOFElements++
			if mem.numberOFElements/mem.Capacity >= mem.Trashold {
				return mem.Flush()
			}
		} else {
			found.Value = record
		}
	}
	return nil
}

func (mem *MemTable) Flush() *[]*record.Record {
	listRecords := make([]*record.Record, 0)

	if mem.StructName == "btree" {
		mem.bTree.InOrderTraversal(&listRecords, mem.bTree.Root)
	} else {
		listRecords = mem.skipList.GetAllElements()
	}

	mem.Empty()
	return &listRecords
}
