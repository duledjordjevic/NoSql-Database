package main

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
	lru "NAiSP/Structures/LRUcache"
	lsm "NAiSP/Structures/LSM"
	memtable "NAiSP/Structures/Memtable"
	readpath "NAiSP/Structures/ReadPath"
	record "NAiSP/Structures/Record"
	wal "NAiSP/Structures/WAL"
	writepath "NAiSP/Structures/WritePath"
	tester "NAiSP/Test"
	"bytes"
	"fmt"
	// bloomfilter "NAiSP/Structures/Bloomfilter"
	// memtable "NAiSP/Structures/Memtable"
	// record "NAiSP/Structures/Record"
	// wal "NAiSP/Structures/WAL"
	// writepath "NAiSP/Structures/WritePath"
)

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

	memTable := memtable.CreateMemtable(10, 1, "btree")
	lru := lru.NewLRUCache(10)
	BF := bloomfilter.NewBLoomFilter(1000, 0.1)
	// BF.Decode("./Data/DataSingle/SizeTiered/bloomfilter.gob")

	wp := writepath.WritePath{
		Wal:         wal.NewWal(),
		MemTable:    memTable,
		BloomFilter: BF,
		Config:      &config,
	}

	list := make([]string, 0)
	// list1 := make([]string, 0)
	// list2 := make([]string, 0)
	// list3 := make([]string, 0)
	novicount := 0
	for i := 0; i < 2000; i++ {
		rec := tester.RandomRecord()
		// fmt.Println("E napravio sam ovaj rekord: ", rec.GetKey())
		list = append(list, rec.GetKey())
		// if len(list) <= 80 {
		// 	list1 = append(list1, rec.GetKey())
		// }
		// if len(list) > 80 && len(list) <= 160 {
		// 	list2 = append(list2, rec.GetKey())
		// }
		// if len(list) > 160 && len(list) <= 240 {
		// 	list3 = append(list3, rec.GetKey())
		// }
		novicount++
		wp.Write(rec)
	}
	// sort.Sort(sort.StringSlice(list1))
	// sort.Sort(sort.StringSlice(list2))
	// sort.Sort(sort.StringSlice(list3))
	lsm.SizeTiered(&config)
	// fmt.Println("LISTA 1: ")
	// for j, i := range list1 {
	// 	fmt.Println("Index: ", j, " element: ", i)
	// }
	// fmt.Println("LISTA 2: ")
	// for j, i := range list2 {
	// 	fmt.Println("Index: ", j, " element: ", i)
	// }
	// fmt.Println("LISTA 3: ")
	// for j, i := range list3 {
	// 	fmt.Println("Index: ", j, " element: ", i)
	// }
	rp := readpath.ReadPath{
		MemTable:     memTable,
		Lru:          lru,
		BloomFilter:  BF,
		ConfigReader: &config,
	}
	BF.Encode("./Data/DataSingle/SizeTiered/bloomfilter.gob")

	count := 0
	for i, key := range list {
		if i%40 == 0 {
			fmt.Println("-------------------------------------------------")
		}
		value := rp.Read(key)
		fmt.Println("Indeks: ", i+1, "Za kljuc: ", key, " sam nasao vrednost: ", value)
		if bytes.Equal(value, []byte{}) {
			count++
		}
	}
	fmt.Println("##############################")
	fmt.Println("Broj praznih bajtova: ", count)
	fmt.Println("##############################")

	// ssTable := sstable.NewSStableFromTOC("./Data/DataMultiple/SizeTiered/Toc/TOC_l3_2.txt")
	// sstable.PrintSummary(ssTable.SummaryPath)
	// sstable.PrintIndexTable(ssTable.IndexTablePath)
	// app := application.CreateApp()
	// // app.Start()
	// menu.Start(app)
}
