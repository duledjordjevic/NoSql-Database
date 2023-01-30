package sstable

import (
	"NAiSP/Structures/record"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type SStable struct {
	DataTablePath  string
	IndexTablePath string
	SummaryPath    string
}

func NewSStable(dataTable string, indexTable string, summary string) *SStable {
	sstable := SStable{DataTablePath: dataTable, IndexTablePath: indexTable, SummaryPath: summary}
	return &sstable
}

func (table *SStable) FormDataIndexSummary(records []record.Record) {
	file, err := os.Create(table.DataTablePath)
	if err != nil {
		fmt.Println("Error")
		return
	}
	defer file.Close()
	fileIndex, err := os.Create(table.IndexTablePath)
	if err != nil {
		fmt.Println("Error")
		return
	}
	defer fileIndex.Close()
	fileSum, err := os.Create(table.SummaryPath)
	if err != nil {
		fmt.Println("Error")
	}
	defer fileSum.Close()

	writer := bufio.NewWriter(file)
	writerIndex := bufio.NewWriter(fileIndex)
	writerSum := bufio.NewWriter(fileSum)
	i := 1
	var firstRecord record.Record
	var lastRecord record.Record
	for _, record := range records {
		if i == 1 {
			firstRecord = record
		}
		if i == len(records) {
			lastRecord = record
		}
		WriteDataTable(record, writer)
		NumOffset, check := getOffsetForKey(table.DataTablePath, record.GetKey())
		if !check {
			continue
		}
		var offset bytes.Buffer
		binary.Write(&offset, binary.BigEndian, NumOffset)
		index := NewIndex(record.GetKey(), offset.Bytes())
		index.WriteIndexTable(writerIndex)
		i++
	}

	sum := NewSummary(firstRecord.GetKey(), lastRecord.GetKey())
	// str := sum.String()
	// fmt.Println(str)
	sum.WriteSummary(writerSum)
	PrintSummary(table.SummaryPath)
	// PrintDataTable(table.DataTablePath)
	// PrintIndexTable(table.IndexTablePath)
}
