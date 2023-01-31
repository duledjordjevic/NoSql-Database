package main

import (
	sstable "NAiSP/Structures/Sstable"
	"fmt"
)

func main() {
	// test1Record := record.NewRecordKeyValue("a", []byte{123, 31}, byte(0))
	// test2Record := record.NewRecordKeyValue("b", []byte{123, 31}, byte(0))
	// test3Record := record.NewRecordKeyValue("c", []byte{123, 31}, byte(0))
	// test4Record := record.NewRecordKeyValue("e", []byte{123, 31}, byte(0))
	// test5Record := record.NewRecordKeyValue("f", []byte{123, 31}, byte(0))
	// test6Record := record.NewRecordKeyValue("g", []byte{123, 31}, byte(0))
	// test7Record := record.NewRecordKeyValue("h", []byte{123, 31}, byte(0))
	// test8Record := record.NewRecordKeyValue("i", []byte{123, 31}, byte(0))
	// test9Record := record.NewRecordKeyValue("j", []byte{123, 31}, byte(0))
	// test10Record := record.NewRecordKeyValue("k", []byte{123, 31}, byte(0))
	// test11Record := record.NewRecordKeyValue("l", []byte{123, 31}, byte(0))
	// test12Record := record.NewRecordKeyValue("m", []byte{123, 31}, byte(0))
	// test13Record := record.NewRecordKeyValue("n", []byte{123, 31}, byte(0))
	// test14Record := record.NewRecordKeyValue("o", []byte{123, 31}, byte(0))
	// test15Record := record.NewRecordKeyValue("p", []byte{123, 31}, byte(0))
	// test16Record := record.NewRecordKeyValue("q", []byte{123, 31}, byte(0))
	// test17Record := record.NewRecordKeyValue("s", []byte{123, 31}, byte(0))
	// lista := []record.Record{*test1Record, *test2Record, *test3Record, *test4Record, *test5Record, *test6Record, *test7Record, *test8Record, *test9Record, *test10Record, *test11Record, *test12Record, *test13Record, *test14Record, *test15Record, *test16Record, *test17Record}

	// tabela := sstable.NewSStable("data.bin", "index.bin", "summary.bin", "bloom.gob", "metadata.txt", "TOC.txt")
	// tabela.FormSStable(lista)
	// record := tabela.Search("l")
	// rec1 := tabela.Search("q")
	// rec2 := tabela.Search("s")
	// rec3 := tabela.Search("d")
	// rec4 := tabela.Search("n")
	// rec5 := tabela.Search("a")

	// fmt.Println(record)
	// fmt.Println(rec1)
	// fmt.Println(rec2)
	// fmt.Println(rec3)
	// fmt.Println(rec4)
	// fmt.Println(rec5)
	tocTabela := sstable.NewSStableFromTOC("TOC.txt")
	fmt.Println(tocTabela.DataTablePath, tocTabela.BloomFilterPath, tocTabela.IndexTablePath, tocTabela.SummaryPath)
}
