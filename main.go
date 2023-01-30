package main

import (
	memtable "NAiSP/Structures/Memtable"
	"NAiSP/Structures/Record"
	"fmt"
)

func main() {

	mem := memtable.MemTable{
		Capacity:   5,
		Trashold:   0.8,
		StructName: "btree",
	}
	listRecords := make([]*Record.Record, 0)
	record1 := Record.NewRecordKeyValue("dusa", []byte{100, 20}, 0)

	record2 := Record.NewRecordKeyValue("traj", []byte{100, 20}, 0)

	record3 := Record.NewRecordKeyValue("niko", []byte{100, 20}, 0)

	record4 := Record.NewRecordKeyValue("rade", []byte{100, 20}, 0)

	record5 := Record.NewRecordKeyValue("stef", []byte{100, 20}, 0)

	record6 := Record.NewRecordKeyValue("mark", []byte{100, 20}, 0)

	listRecords = append(listRecords, record1)
	listRecords = append(listRecords, record2)
	listRecords = append(listRecords, record3)
	listRecords = append(listRecords, record4)
	listRecords = append(listRecords, record5)
	listRecords = append(listRecords, record6)
	brojac := 1
	for _, i := range listRecords {
		found := mem.Add(i)
		if found == nil {
			fmt.Println("Flash", brojac)
			for _, j := range *found {
				fmt.Println(j)
			}
			brojac++
		}
	}

	// record7 := Record.NewRecordKeyValue("buca", []byte{100, 20}, 0)
	// found = mem.Add(record7)

	// record8 := Record.NewRecordKeyValue("kasa", []byte{100, 20}, 0)
	// found = mem.Add(record8)

	// record9 := Record.NewRecordKeyValue("rada", []byte{100, 20}, 0)
	// found = mem.Add(record9)

	// record10 := Record.NewRecordKeyValue("kada", []byte{100, 20}, 0)
	// found = mem.Add(record10)

	// record11 := Record.NewRecordKeyValue("kara", []byte{100, 20}, 0)
	// found = mem.Add(record11)

	// record12 := Record.NewRecordKeyValue("maaa", []byte{100, 20}, 0)
	// found = mem.Add(record12)

	// record13 := Record.NewRecordKeyValue("mata", []byte{100, 20}, 0)
	// found = mem.Add(record13)

}
