package main

import (
	application "NAiSP/App"
	menu "NAiSP/Menu"
)

// bloomfilter "NAiSP/Structures/Bloomfilter"
// memtable "NAiSP/Structures/Memtable"
// record "NAiSP/Structures/Record"
// wal "NAiSP/Structures/WAL"
// writepath "NAiSP/Structures/WritePath"

// tester "NAiSP/Test"

func main() {
	app := application.CreateApp()
	menu.Start(app)
}
