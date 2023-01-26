package memtable

import (
	record "NAiSP/Structures/Record"
	skiplist "NAiSP/Structures/Skiplist"
)

type MemTable struct {
	Capacity         float64
	Trashold         float64
	StructName       string
	NumberOFElements float64
	Structure        skiplist.Structure
}

func (mem *MemTable) FillDefaults() {

	if mem.Capacity == 0 {
		mem.Capacity = 100
	}

	if mem.Trashold == 0 {
		mem.Trashold = 0.8
	}

	if mem.NumberOFElements != 0 {
		mem.NumberOFElements = 0
	}

	if mem.StructName == "btree" {
		// mem.Structure = btree.CreateBTree()
	} else {
		mem.Structure = skiplist.CreateSkipList()
	}
}

func (mem *MemTable) Empty() {
	mem.FillDefaults()
}

func (mem *MemTable) Find(key string) bool {
	found := mem.Structure.Search(key)
	if found.Value.GetKey() == key {
		if found.Value.GetTombStone() == 1 {
			return false
		} else {
			return true
		}
	}
	return false
}

func (mem *MemTable) Add(record *record.Record) {

	found := mem.Structure.AddElement(record)
	if found.Value.GetKey() == record.GetKey() && found.Value.GetTimeStamp() == record.GetTimeStamp() && found.Value.GetTombStone() == record.GetTombStone() && string(found.Value.GetValue()) == string(record.GetValue()) {
		mem.NumberOFElements++
		if mem.NumberOFElements/mem.Capacity >= mem.Trashold {
			mem.Flush()
		}
	} else {
		found.Value = record
	}
}

func (mem *MemTable) Flush() []*record.Record {
	listRecords := make([]*record.Record, 0)

	if mem.StructName == "btree" {

	} else {
		listRecords = mem.Structure.GetAllElements()
	}

	mem.Empty()
	return listRecords
}
