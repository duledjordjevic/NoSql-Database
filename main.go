package main

import (
	application "NAiSP/App"
	"fmt"
)

// bloomfilter "NAiSP/Structures/Bloomfilter"
// memtable "NAiSP/Structures/Memtable"
// record "NAiSP/Structures/Record"
// wal "NAiSP/Structures/WAL"
// writepath "NAiSP/Structures/WritePath"

// tester "NAiSP/Test"

func main() {
	app := application.CreateApp()
	fmt.Println(app.ReadValue("inexi :"))
}
