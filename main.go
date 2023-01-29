package main

import (
	btree "NAiSP/Structures/Btree"
	"NAiSP/Structures/Record"
)

func main() {
	bTree := btree.CreateBTree(4)
	record1 := Record.NewRecordKeyValue("dusa", []byte{100, 20}, 0)
	bTree.AddElement(record1, nil)
	// keys := bTree.AddElement(record1).keys
	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }
	record2 := Record.NewRecordKeyValue("traj", []byte{100, 20}, 0)
	bTree.AddElement(record2, nil)
	// keys := bTree.AddElement(record2).keys

	record3 := Record.NewRecordKeyValue("niko", []byte{100, 20}, 0)
	bTree.AddElement(record3, nil)
	// keys := bTree.AddElement(record3).keys
	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }

	record4 := Record.NewRecordKeyValue("rade", []byte{100, 20}, 0)
	bTree.AddElement(record4, nil)

	// keys = bTree.AddElement(record4).keys

	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }

	record5 := Record.NewRecordKeyValue("stef", []byte{100, 20}, 0)
	bTree.AddElement(record5, nil)
	// keys = bTree.AddElement(record5).parent.keys
	// fmt.Println(bTree.AddElement(record5).parent.children[0].keys)
	// for i := 0; i < 3; i++ {
	// 	fmt.Println(keys[i])
	// }

	record6 := Record.NewRecordKeyValue("mark", []byte{100, 20}, 0)
	bTree.AddElement(record6, nil)

	record7 := Record.NewRecordKeyValue("buca", []byte{100, 20}, 0)
	bTree.AddElement(record7, nil)

	record8 := Record.NewRecordKeyValue("kasa", []byte{100, 20}, 0)
	bTree.AddElement(record8, nil)

	record9 := Record.NewRecordKeyValue("rada", []byte{100, 20}, 0)
	bTree.AddElement(record9, nil)

	record10 := Record.NewRecordKeyValue("kada", []byte{100, 20}, 0)
	bTree.AddElement(record10, nil)

	record11 := Record.NewRecordKeyValue("kara", []byte{100, 20}, 0)
	bTree.AddElement(record11, nil)

	record12 := Record.NewRecordKeyValue("maaa", []byte{100, 20}, 0)
	bTree.AddElement(record12, nil)

	record13 := Record.NewRecordKeyValue("mata", []byte{100, 20}, 0)
	bTree.AddElement(record13, nil)

	record14 := Record.NewRecordKeyValue("sten", []byte{100, 20}, 0)
	bTree.AddElement(record14, nil)

	record15 := Record.NewRecordKeyValue("stem", []byte{100, 20}, 0)
	bTree.AddElement(record15, nil)

	record16 := Record.NewRecordKeyValue("tema", []byte{100, 20}, 0)
	bTree.AddElement(record16, nil)

	record17 := Record.NewRecordKeyValue("anat", []byte{100, 20}, 0)
	bTree.AddElement(record17, nil)

	record18 := Record.NewRecordKeyValue("anab", []byte{100, 20}, 0)
	bTree.AddElement(record18, nil)

	record19 := Record.NewRecordKeyValue("mala", []byte{100, 20}, 0)
	bTree.AddElement(record19, nil)

	record20 := Record.NewRecordKeyValue("anaz", []byte{100, 20}, 0)
	bTree.AddElement(record20, nil)

	record21 := Record.NewRecordKeyValue("matr", []byte{100, 20}, 0)
	bTree.AddElement(record21, nil)

	record22 := Record.NewRecordKeyValue("palm", []byte{100, 20}, 0)
	bTree.AddElement(record22, nil)

	record23 := Record.NewRecordKeyValue("ssss", []byte{100, 20}, 0)
	bTree.AddElement(record23, nil)

	record24 := Record.NewRecordKeyValue("tsts", []byte{100, 20}, 0)
	bTree.AddElement(record24, nil)

	record25 := Record.NewRecordKeyValue("tstm", []byte{100, 20}, 0)
	bTree.AddElement(record25, nil)

	record26 := Record.NewRecordKeyValue("tsta", []byte{100, 20}, 0)
	bTree.AddElement(record26, nil)

	record27 := Record.NewRecordKeyValue("tstb", []byte{100, 20}, 0)
	bTree.AddElement(record27, nil)

	record28 := Record.NewRecordKeyValue("tstr", []byte{100, 20}, 0)
	bTree.AddElement(record28, nil)

	record29 := Record.NewRecordKeyValue("tste", []byte{100, 20}, 0)
	bTree.AddElement(record29, nil)

	record30 := Record.NewRecordKeyValue("tstp", []byte{100, 20}, 0)
	bTree.AddElement(record30, nil)

	record31 := Record.NewRecordKeyValue("tstz", []byte{100, 20}, 0)
	bTree.AddElement(record31, nil)
	// fmt.Println(bTree.Root.Children[0].Keys)
	// fmt.Println(bTree.Root.Children[2].Keys)
	bTree.Print(bTree.Root)
}
