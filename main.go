package main

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
	lsm "NAiSP/Structures/LSM"
	memtable "NAiSP/Structures/Memtable"
	record "NAiSP/Structures/Record"
	wal "NAiSP/Structures/WAL"
	writepath "NAiSP/Structures/WritePath"
	tester "NAiSP/Test"
	"fmt"
)

// tester "NAiSP/Test"

func main() {
	test1Record := record.NewRecordKeyValue("a", []byte{123, 31}, byte(0))
	test2Record := record.NewRecordKeyValue("b", []byte{123, 31}, byte(0))
	test3Record := record.NewRecordKeyValue("c", []byte{123, 31}, byte(0))
	test4Record := record.NewRecordKeyValue("e", []byte{123, 31}, byte(0))
	test5Record := record.NewRecordKeyValue("f", []byte{123, 31}, byte(0))
	test6Record := record.NewRecordKeyValue("g", []byte{123, 31}, byte(0))
	test7Record := record.NewRecordKeyValue("h", []byte{123, 31}, byte(0))
	test8Record := record.NewRecordKeyValue("i", []byte{123, 31}, byte(0))
	test9Record := record.NewRecordKeyValue("j", []byte{123, 31}, byte(0))
	test10Record := record.NewRecordKeyValue("k", []byte{123, 31}, byte(0))
	test11Record := record.NewRecordKeyValue("l", []byte{123, 31}, byte(0))
	test12Record := record.NewRecordKeyValue("m", []byte{123, 31}, byte(0))
	test13Record := record.NewRecordKeyValue("n", []byte{123, 31}, byte(0))
	test14Record := record.NewRecordKeyValue("o", []byte{123, 31}, byte(0))
	test15Record := record.NewRecordKeyValue("p", []byte{123, 31}, byte(0))
	test16Record := record.NewRecordKeyValue("q", []byte{123, 31}, byte(0))
	test17Record := record.NewRecordKeyValue("s", []byte{123, 31}, byte(0))
	test18Record := record.NewRecordKeyValue("y", []byte{123, 31}, byte(0))
	test19Record := record.NewRecordKeyValue("w", []byte{123, 31}, byte(0))
	test20Record := record.NewRecordKeyValue("z", []byte{123, 31}, byte(0))
	// // lista := []record.Record{*test1Record, *test2Record, *test3Record, *test4Record, *test5Record, *test6Record, *test7Record, *test8Record, *test9Record, *test10Record, *test11Record, *test12Record, *test13Record, *test14Record, *test15Record, *test16Record, *test17Record}
	lista1 := make([]*record.Record, 0)
	lista1 = append(lista1, test11Record)
	lista1 = append(lista1, test12Record)
	lista1 = append(lista1, test13Record)
	lista1 = append(lista1, test14Record)
	lista1 = append(lista1, test15Record)
	lista1 = append(lista1, test6Record)
	lista1 = append(lista1, test7Record)
	lista1 = append(lista1, test8Record)
	lista1 = append(lista1, test9Record)
	lista1 = append(lista1, test10Record)
	lista1 = append(lista1, test16Record)
	lista1 = append(lista1, test17Record)
	lista1 = append(lista1, test18Record)
	lista1 = append(lista1, test19Record)
	lista1 = append(lista1, test20Record)
	lista1 = append(lista1, test1Record)
	lista1 = append(lista1, test2Record)
	lista1 = append(lista1, test3Record)
	lista1 = append(lista1, test4Record)
	lista1 = append(lista1, test5Record)

	config := configreader.ConfigReader{}
	config.ReadConfig()

	capacity := float64(0)
	var rec *record.Record

	BF := bloomfilter.BloomFilter{}
	WAL := wal.NewWal()
	MemTable := memtable.CreateMemtable(10, 1, "btree")
	wp := writepath.WritePath{Wal: WAL, MemTable: MemTable, BloomFilter: &BF, Config: &config}

	for _, rec = range lista1 {
		wp.Write(rec)
		capacity += float64(rec.GetSize())
	}

	testRecords := make([]*record.Record, 0)

	for i := 0; i < 100; i++ {
		rec = tester.RandomRecord()
		wp.Write(rec)
		capacity += float64(rec.GetSize())
		testRecords = append(testRecords, rec)
	}

	LSM := lsm.LSM{Config: &config}
	Level := lsm.NewLeveled(&config, &LSM)
	Level.Compaction()

	fmt.Println("Ukupan broj fajlova -> ", capacity/1024)

	// for i, rec := range testRecords {
	// 	fmt.Print("Test ", i)
	// 	fmt.Println(" -> ", )
	// }

	// path := "./Data/DataMultiple/Leveled/Data/data_l1_ABC.bin"
	// counter := 0
	// for {
	// 	filepath := strings.ReplaceAll(path, "ABC", strconv.FormatInt(int64(counter), 10))
	// 	err := tester.ReadFile(filepath)
	// 	if err != nil {
	// 		break
	// 	}
	// 	counter++
	// }

	// _ = tester.ReadFile("./Data/DataMultiple/Leveled/Data/data_l0_11.bin")

	// fmt.Println(writepath.GenerateFileName("size_tiered"))
	// lsm.SizeTiered(&config)
}
